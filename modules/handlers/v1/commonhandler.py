# -*- coding: utf-8 -*-

from __future__ import print_function, division

import re
import time
import copy
from collections import MutableMapping
from abc import abstractmethod
from string import Template

from elasticsearch import Elasticsearch, ConflictError, NotFoundError
from elasticsearch.client import IndicesClient
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan, bulk
from zope.interface import implementer

import application.app as app
import modules.remote as remote
import modules.handlers.common as common

from application.connection import ConnectinoPool
from application import IllegalConfigException
from modules.interfaces import IHandler
from ...handlers import INSERT, UPDATE, DELETE, COMMON

_logger = app.getLogger('base')

"""
"""
_EXP_RE = re.compile(r"(%%)|%(?:(\w+)\.)?(\w+)(?::\(((?:[^'\)][^\)]*)|(?:'.*?[^\\]'))\))?")

"""
"""
_ORIGIN_VALUE_RE = re.compile(r"^%(?:(\w+)\.)?(\w+)(?::\(((?:[^'\)][^\)]*)|(?:'.*?[^\\]'))\))?$")

"""
"""
_PARENT_EXP_RE = re.compile(r"(\w+)\s*=\s*%__parent\.(\w+)|%__parent\.(\w+)\s*=\s*(\w+)")

"""
"""
_MAX_RETRY_COUNT = 256

@implementer(IHandler)
class CommonHandler(object):
    def __init__(self, statusConfig):
        self._statusConfig = statusConfig
        self._binlogHandler = BinlogHandler(self._statusConfig)

    def syncFromMySQL(self):
        handlerConfig = self._statusConfig.handlerConfig
        for configList in handlerConfig:
            handler = MySQLHandler(configList, self._statusConfig)
            handler.sync()
    
    def syncFromBinlog(self, binlogEvent):
        self._binlogHandler.sync(binlogEvent)

class _ElasticSearchUtilsMixin(object):
    """
    need instances:
    self._statusConfig
    self._esClient
    self._esIndexClient
    """

    def _writeToIndex(self, masterItem, document, context):
        documentId = context.exp_value(masterItem['document_id'], masterItem)

        routing = masterItem.get('routing', None)
        if routing:
            routing = context.exp_value(routing, masterItem)

        _logger.debug('write document: id[%s], routing[%s], document[%s]', documentId, routing, document)

        self._esClient.index(
                index=self._getESIndexFullname(masterItem.esIndex),
                doc_type=masterItem.esType,
                body=document,
                id=documentId,
                routing=routing
                )

    def _deleteFromIndex(self, masterItem, context):
        documentId = context.exp_value(masterItem['document_id'], masterItem)
        routing = masterItem.get('routing', None)
        if routing:
            routing = context.exp_value(routing, masterItem)
            
        try:
            self._esClient.delete(
                    index=self._getESIndexFullname(masterItem.esIndex),
                    doc_type=masterItem.esType,
                    id=documentId,
                    routing=routing
                    )
        except NotFoundError as e:
            # Don't raise NotFoundError
            _logger.error('document with id[%s] and routing[%s] in index[%s] Not Found', documentId, routing, masterItem.esIndex)
        except:
            raise

    def _getESIndexFullname(self, index):
        return index + '_' + self._statusConfig.esIndexSuffix
             
    def _updateByQuery(self, esIndex, esType, body):
        esIndex = self._getESIndexFullname(esIndex)

        retry = 0
        while retry < _MAX_RETRY_COUNT:
            try:
                _logger.debug('update by query: param[%s, %s, %s]', esIndex, esType, body)

                result = self._esClient.update_by_query(
                        index=esIndex,
                        doc_type=esType,
                        body=body,
                        conflicts='abort'
                        )

                _logger.debug('update by query successfully: result[%s], retry[%s]', result, retry)

                return result
            except ConflictError as e:
                retry += 1
                _logger.error('conflict error occurs when _updateByQuery: %s, %s, %s, retry[%s] ', esIndex, esType, body, retry)

                time.sleep(retry * 0.2)
                self._esIndexClient.refresh(index=esIndex)

class _HandlerUtilsMixin(object):
    def _getParentQuery(self, nestedItem, values):
        parentQuery = nestedItem.get('parent_query', None)
        if parentQuery:
            itemKey = nestedItem.key
            context = HandlerContext(nestedItem, { itemKey: values })
            parentQuery = context.exp_data(parentQuery, nestedItem)
            return parentQuery

        statement = nestedItem['statement']
        matches = _PARENT_EXP_RE.findall(statement)
        if not matches:
            raise IllegalConfigException('could NOT find parent dependence in nested item: %s' % nestedItem)

        # 构造parent values的部分值
        parentValues = {}
        for match in matches:
            f1, f2, f3, f4 = match
            if f1 and f2:
                parentValues[f2] = values.get(f1, None)
            elif f3 and f4:
                parentValues[f3] = values.get(f3, None)

        parentItem = nestedItem.getLocatedConfigList().getParentItem()
        parentKey = parentItem.key
        context = HandlerContext(parentItem, { parentKey: parentValues })

        # TODO 这里有个bug：当parentItem是master item时，可能不存在query配置
        # 暂时的解决方案是在nestedItem中增加配置parent_query
        parentQuery = context.exp_data(parentItem['query'], parentItem)
        return parentQuery

    def _getDiffFields(self, beforeValues, afterValues):
        fields = []
        for field in beforeValues.keys():
            if beforeValues[field] != afterValues[field]:
                fields.append(field)

        return fields

    def _getBoolQuery(self, query):
        boolQuery = {
                "bool": {
                    "filter": []
                    }
                }
        
        for key, value in query.items():
            boolQuery['bool']['filter'].append({
                'term': { key: value }
                })
        
        return boolQuery

    def _getNestedBoolQuery(self, nestedItem):
        query = nestedItem['query']
        nestedList = nestedItem.getLocatedConfigList()
        parentField = nestedList.getParentField()

        newQuery = {}
        for key, value in query.items():
            newQuery[parentField + '.' + key] = value

        nestedQuery = { 
                "nested": {
                    "path": parentField
                    }
                }
        boolQuery = self._getBoolQuery(newQuery)
        nestedQuery['nested']['query'] = boolQuery

        return nestedQuery

class MySQLHandler(_ElasticSearchUtilsMixin, object):
    def __init__(self, configList, statusConfig):
        self._configList = configList
        self._statusConfig = statusConfig

        self._esIndex = configList.esIndex
        self._esType = configList.esType

        self._esClient = remote.getElasticClient()
        self._esIndexClient = IndicesClient(self._esClient)

    def sync(self):
        print('begin to sync index[%s, %s] from mysql' % (self._esIndex, self._esType))

        startTime = time.time()

        count = 0
        masterItem = self._configList.getMasterItem()
        dataFetcher = MySQLDataFetcher(self._configList)
        for doc, context in dataFetcher.buildDocument():
            # 写入ES
            self._writeToIndex(masterItem, doc, context)

            count += 1
            if count % 50 == 0:
                print('%s rows have been done' % (count,))

        endTime = time.time()
        print('finish to sync index[%s, %s] from mysql, count[%s], time cost[%s]\n\n' % (self._esIndex, self._esType, count, endTime - startTime))

class MySQLDataFetcher(object):
    def __init__(self, configList, **predefinedCtx):
        self._configList = configList
        self._predefinedCtx = predefinedCtx

        self._configItems = { item.key: item for item in configList.getAllItems() }

    def getAllDocuments(self):
        docs = []
        for doc, context in self.buildDocument():
            docs.append(doc)
            if len(docs) > 50:
                raise LogicException('get too many (greater than 50) documents at one time')
        return docs

    def buildDocument(self):
        masterKey = self._configList.getMasterKey()

        context = HandlerContext(self._configItems)
        while True:
            # 获取上一次的主表记录，并重新初始化context
            lastMasterRow = context.get(masterKey, {})

            # 重新初始化context
            context.clearData()
            context.update(self._predefinedCtx)
            context.update({ '__last': lastMasterRow })

            # 首先获取主表的一条记录
            row = context.getData(masterKey)
            if not row:
                # 主表记录为空，那么返回
                break

            document = context.fillDataToDocument()

            yield document, context

class HandlerContext(MutableMapping):
    def __init__(self, configs, data=None):
        self._data = data if data else {}
        self._nestedData = {}
        self._connPool = ConnectinoPool()
        self._masterKey = None

        if configs:
            if isinstance(configs, dict):
                self._configs = configs
                for key in configs:
                    if configs[key].isMaster:
                        self._masterKey = key
            elif isinstance(configs, list) or isinstance(configs, tuple) or isinstance(configs, set):
                self._configs = {}
                for item in configs:
                    self._configs[item.key] = item
                    if item.isMaster:
                        self._masterKey = item.key
            else:
                self._configs = { configs.key: configs }
                if configs.isMaster:
                    self._masterKey = configs.key
        else:
            self._configs = {}

    def __getitem__(self, key):
        if key == '__master':
            return self._data[self._masterKey]

        return self._data[key]

    def __setitem__(self, key, value):
        if key == '__master':
            self._data[self._masterKey] = value
        else:
            self._data[key] = value

    def __delitem__(self, key):
        if key == '__master':
            key = self._masterKey

        if key in self._data:
            del self._data[key]
            
    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def clearData(self):
        self._data = {}
        return self

    def fillDataToDocument(self):
        document = {}

        for config in self._configs.values():
            key = config['key']
            mapping = config['mapping']
            data = self.getData(key)
            
            for item in mapping:
                dbField = item['db_field']
                esField = item['es_field']
                itemType = item['type']
                nullValue = item['null_value']

                if itemType == 'nested':
                    document[esField] = self.getNestedData(key, esField)
                else:
                    document[esField] = CommonUtils.getDBFieldValue(dbField, data, config, nullValue)
        
        return document

    def getData(self, key):
        if key in self:
            return self[key]

        if key == '__master':
            key = self._masterKey

        if key not in self._configs:
            raise LogicException('data key[%s] must be in self._configs' % key)

        config = self._configs[key]
        self.executeStatement(config)

        if key not in self:
            raise LogicException('data NOT found in current context: key[%s], context[%s]' % (key, self))

        return self[key]

    def getNestedData(self, key, esField):
        _logger.debug('get nested data: %s, %s', key, esField)

        parentData = self.getData(key)
        nestedDataKey = '__nested_' + esField
        if nestedDataKey in parentData:
            return parentData[nestedDataKey]
 
        # 获取nested
        config = self._configs.get(key, None)
        if config.hasNested() and parentData:
            nestedList = config.getNestedListByEsField(esField)
            if nestedList:
                fetcher = MySQLDataFetcher(nestedList, __parent=parentData)
                nestedDocs = fetcher.getAllDocuments()
                parentData[nestedDataKey] = nestedDocs
                self[key] = parentData
                return nestedDocs

        return None

    def executeStatement(self, config):
        """
        执行配置节(ConfigItem)中的MySQL语句
        """
        key = config['key']
        database = config['database']
        table = config['table']
        statement = config['statement']

        if key in self:
            return self[key]

        _logger.debug('statement origin value: %s', statement)
        statement = self.exp_value(statement, config)
        _logger.debug('statement exp value: %s', statement)

        data = None
        if statement:
            statement += ' LIMIT 1 '

            _logger.debug('executeStatement: %s', statement)

            conn = self._connPool.connection(database)
            with conn.cursor() as cursor:
                cursor.execute(statement)
                data = cursor.fetchall()
                _logger.debug('executeStatement data: %s', data)

        self[key] = data[0] if data else {}

        return self[key]

    def exp_data(self, data, config, recursive=False, deepcopy=True):
        if deepcopy:
            data = copy.deepcopy(data)

        if isinstance(data, list):
            for index, item in enumerate(data):
                data[index] = self.exp_data(item, config, recursive=recursive, deepcopy=False)
        elif isinstance(data, dict):
            for key, item in data.iteritems():
                data[key] = self.exp_data(item, config, recursive=recursive, deepcopy=False)
        elif isinstance(data, basestring):
            data = self.exp_value(data, config, recursive=recursive)
        else:
            pass # do nothing, just return data

        return data

    def exp_value(self, value, currConfig, recursive=True):
        def get_origin_value(key, fieldName, defaultValue):
            _logger.debug('get_origin: key[%s], fieldName[%s], default[%s]', key, fieldName, defaultValue)

            defaultValue = common.echo(defaultValue)
            if not key:
                key = currConfig['key']

            _logger.debug('get_origin: key[%s], fieldName[%s], default[%s]', key, fieldName, defaultValue)

            if recursive:
                if key not in self:
                    oc = self._configs.get(key, None)
                    if not oc:
                        raise LogicException('exp value NOT found: [%s]' % value)
                    self.executeStatement(oc)

            if key not in self:
                raise LogicException('exp value NOT found: [%s]' % value)

            data = self[key]
            if not data:
                return defaultValue
            
            ret = data.get(fieldName, defaultValue)
            return ret

        def sub_exp(matchobj):
            if matchobj.group(0) == '%%':
                return '%'

            key = matchobj.group(2)
            fieldName = matchobj.group(3)
            defaultValue = matchobj.group(4)

            ret = get_origin_value(key, fieldName, defaultValue)
            return unicode(ret)

        _logger.debug('exp_value: value[%s], currconfig[%s]', value, currConfig)

        matches = _ORIGIN_VALUE_RE.match(value)
        if matches:
            key = matches.group(1)
            fieldName = matches.group(2)
            defaultValue = matches.group(3)
            value = get_origin_value(key, fieldName, defaultValue)
        else:
            value = _EXP_RE.sub(sub_exp, value)

        _logger.debug('exp_value result: value[%s], context[%s]', value, self)
        return value

    def __str__(self):
        return str({
            '_configs': self._configs.keys(),
            '_data': self._data
            })

class BinlogHandler(object):
    def __init__(self, statusConfig):
        self._statusConfig = statusConfig
        self._handlerConfig = statusConfig.handlerConfig

        self._insertEventProcessor = InsertEventProcessor(statusConfig)
        self._deleteEventProcessor = DeleteEventProcessor(statusConfig)
        self._updateEventProcessor = UpdateEventProcessor(
                statusConfig, 
                self._insertEventProcessor, 
                self._deleteEventProcessor
                )

    def sync(self, binlogEvent):
        database = binlogEvent['database']
        table = binlogEvent['table']
        configItems = self._handlerConfig.getConfigItemsByDatabaseAndTable(database, table)
        if not configItems:
            return

        _logger.debug('relatived configs of database[%s] and table[%s]: %s', database, table, configItems)

        eventType = binlogEvent['type']
        if eventType == 'INSERT':
            self._syncFromInsertEvent(configItems, binlogEvent)
        elif eventType == 'DELETE':
            self._syncFromDeleteEvent(configItems, binlogEvent)
        elif eventType == 'UPDATE':
            self._syncFromUpdateEvent(configItems, binlogEvent)

    def _syncFromInsertEvent(self, configItems, binlogEvent):
        values = binlogEvent['values']

        for config in configItems:
            filterDict = config.get('filter', None)
            filtered = CommonUtils.filterData(filterDict, values)                    
            if not filtered:
                continue

            _logger.debug('config[%s], binlog[%s]', config, binlogEvent)
            self._insertEventProcessor.process(config, binlogEvent)

    def _syncFromDeleteEvent(self, configItems, binlogEvent):
        values = binlogEvent['values']

        for config in configItems:
            filterDict = config.get('filter', None)
            filtered = CommonUtils.filterData(filterDict, values)                    
            if not filtered:
                continue

            _logger.debug('config[%s], binlog[%s]', config, binlogEvent)
            self._deleteEventProcessor.process(config, binlogEvent)
        
    def _syncFromUpdateEvent(self, configItems, binlogEvent):
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        for config in configItems:
            filterDict = config.get('filter', None)
            _logger.debug('filterDict[%s] of config[%s]', filterDict, config)

            isBeforeFiltered = CommonUtils.filterData(filterDict, beforeValues)
            isAfterFiltered = CommonUtils.filterData(filterDict, afterValues)

            _logger.debug('isBeforeFiltered: %s ; isAfterFiltered: %s', isBeforeFiltered, isAfterFiltered)

            if not isBeforeFiltered and not isAfterFiltered:
                continue

            if not isBeforeFiltered and isAfterFiltered:
                newEventLog = CommonUtils.buildBinlogEventLog(
                        binlogEvent['database'], 
                        binlogEvent['table'],
                        'INSERT', 
                        afterValues
                        )
                self._insertEventProcessor.process(config, newEventLog)
                continue

            if isBeforeFiltered and not isAfterFiltered:
                newEventLog = CommonUtils.buildBinlogEventLog(
                        binlogEvent['database'], 
                        binlogEvent['table'],
                        'DELETE', 
                        beforeValues
                        )
                self._deleteEventProcessor.process(config, newEventLog)
                continue

            self._updateEventProcessor.process(config, binlogEvent)

class _BaseEventProcessor(_ElasticSearchUtilsMixin, _HandlerUtilsMixin, object):
    def __init__(self, statusConfig):
        self._statusConfig = statusConfig
        self._esClient = remote.getElasticClient()
        self._esIndexClient = IndicesClient(self._esClient)

        self._scripts = {}

    def process(self, config, binlogEvent):
        if config.isMaster:
            if config.isNested():
                self._processNestedMasterItem(config, binlogEvent)
            else:
                self._processMasterItem(config, binlogEvent)
        else:
            if config.isNested():
                self._processNestedSlaveItem(config, binlogEvent)
            else:
                self._processSlaveItem(config, binlogEvent)

    def _getScriptKey(self, config, extraKeys):
        esIndex = config.esIndex
        esType = config.esType
        configKey = config.key
        if config.isNested():
            parentKey = config.getLocatedConfigList().getParentKey()
        else:
            parentKey = ''
        key = (esIndex, esType, configKey, parentKey)

        if extraKeys:
            key += tuple(sorted(extraKeys))

        return key

    def _getInlineScript(self, config, extraKeys=None):
        key = self._getScriptKey(config, extraKeys)
        return self._scripts.get(key, None)

    def _setInlineScript(self, config, script, extraKeys=None):
        key = self._getScriptKey(config, extraKeys)
        self._scripts[key] = script

    @abstractmethod
    def _processMasterItem(self, config, binlogEvent):
        pass

    @abstractmethod
    def _processNestedMasterItem(self, config, binlogEvent):
        pass

    @abstractmethod
    def _processSlaveItem(self, config, binlogEvent):
        pass

    @abstractmethod
    def _processNestedSlaveItem(self, config, binlogEvent):
        pass

class InsertEventProcessor(_BaseEventProcessor):
    def __init__(self, statusConfig):
        super(InsertEventProcessor, self).__init__(statusConfig)

    def _processMasterItem(self, config, binlogEvent):
        """
        InsertEventProcessor
        """
        values = binlogEvent['values']

        masterKey = config.key
        configList = config.getLocatedConfigList()

        context = HandlerContext(configList.getAllItems(), { masterKey: values })
        document = context.fillDataToDocument()

        self._writeToIndex(config, document, context)

    def _processNestedMasterItem(self, config, binlogEvent):
        """
        InsertEventProcessor
        """
        values = binlogEvent['values']

        masterKey = config.key
        configList = config.getLocatedConfigList()

        context = HandlerContext(configList.getAllItems(), { masterKey: values })
        document = context.fillDataToDocument()

        script = self._getNestedMasterItemScript(config, document)

        parentQuery = self._getParentQuery(config, values)
        body = {
                'query': self._getBoolQuery(parentQuery),
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _processSlaveItem(self, config, binlogEvent):
        """
        InsertEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, withSelf=True)

        context = HandlerContext(relativedConfigs, { configKey: values })

        query = context.exp_data(config['query'], config)
        script = self._getSlaveItemScript(config, relativedConfigs, context)

        body = {
                'query': self._getBoolQuery(query),
                'script': script
                }

        self._updateByQuery(
                config.esIndex,
                config.esType,
                body
                )

    def _processNestedSlaveItem(self, config, binlogEvent):
        """
        InsertEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: values })

        nestedQuery = self._getNestedBoolQuery(config)
        nestedQuery = context.exp_data(nestedQuery, config)

        script = self._getNestedSlaveItemScript(config, relativedConfigs, context)

        body = {
                'query': nestedQuery,
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _getNestedMasterItemScript(self, config, nestedDoc):
        """
        InsertEventProcessor
        """
        configList = config.getLocatedConfigList()
        parentField = configList.getParentField()

        inlineScript = self._getInlineScript(config)
        if not inlineScript:
            inlineTemplate = Template("if (ctx._source.${field} == null) ctx._source.${field} = []; ctx._source.${field}.add(params.${field})")
            inlineScript = inlineTemplate.substitute(field=parentField)
            self._setInlineScript(config, inlineScript)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': { parentField: nestedDoc }
                }
        return script

    def _getSlaveItemScript(self, config, relativedConfigs, context):
        """
        InsertEventProcessor
        """
        relativedConfigs = relativedConfigs.values()

        inlineScript = self._getInlineScript(config)

        if not inlineScript:
            inlineList = []
            for conf in relativedConfigs:
                mapping = conf['mapping']
                for item in mapping:
                    itemType = item['type']
                    esField = item['es_field']

                    inlineList.append("ctx._source.%s = params.%s" % (esField, esField))

            inlineScript = ';'.join(inlineList)
            self._setInlineScript(config, inlineScript)

        params = {}
        for conf in relativedConfigs:
            confKey = conf['key']
            mapping = conf['mapping']
            values = context.getData(confKey)

            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                itemType = item['type']
                nullValue = item['null_value']
        
                if itemType == 'nested':
                    params[esField] = context.getNestedData(confKey, esField)
                else:
                    params[esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

    def _getNestedSlaveItemScript(self, config, relativedConfigs, context):
        """
        InsertEventProcessor
        """
        query = context.exp_data(config['query'], config)

        inlineScript = self._getInlineScript(config)
        if not inlineScript:
            inlineTemplate = Template("""
            for (item in ctx._source.${parent_field}) { 
              if (${query_condition}) {
                ${item_evaluation};
              } 
            }
            """)

            queryConditions = []
            for key in query:
                queryConditions.append('item.%s == params.query.%s' % (key, key))

            itemEvaluations = []
            for conf in relativedConfigs.values():
                mapping = conf['mapping']
                for item in mapping:
                    esField = item['es_field']

                    # no nested type in nested config item
                    itemEvaluations.append("item.%s = params.data.%s" % (esField, esField))

            parentField = config.getLocatedConfigList().getParentField()
            inlineScript = inlineTemplate.substitute(
                    parent_field=parentField,
                    query_condition='&&'.join(queryConditions),
                    item_evaluation=';'.join(itemEvaluations)
                    )
            self._setInlineScript(config, inlineScript)

        params = {
                'query': query,
                'data': {}
                }
        for conf in relativedConfigs.values():
            confKey = conf['key']
            mapping = conf['mapping']
            values = context.getData(confKey)
            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                nullValue = item['null_value']
        
                # no nested type in nested config item
                params['data'][esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

class DeleteEventProcessor(_BaseEventProcessor):
    def __init__(self, statusConfig):
        super(DeleteEventProcessor, self).__init__(statusConfig)

    def _processMasterItem(self, config, binlogEvent):
        """
        DeleteEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        context = HandlerContext(config, { configKey: values })

        self._deleteFromIndex(config, context)

    def _processNestedMasterItem(self, config, binlogEvent):
        """
        DeleteEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        context = HandlerContext(config, { configKey: values })

        script = self._getNestedMasterItemScript(config, context)

        nestedQuery = self._getNestedBoolQuery(config)
        nestedQuery = context.exp_data(nestedQuery, config)

        body = {
                'query': nestedQuery,
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _processSlaveItem(self, config, binlogEvent):
        """
        DeleteEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: values })

        script = self._getSlaveItemScript(config, relativedConfigs, context)

        query = context.exp_data(config['query'], config)
        body = {
                'query': self._getBoolQuery(query),
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _processNestedSlaveItem(self, config, binlogEvent):
        """
        DeleteEventProcessor
        """
        values = binlogEvent['values']

        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: values })

        script = self._getNestedSlaveItemScript(config, relativedConfigs, context)

        nestedQuery = self._getNestedBoolQuery(config)
        nestedQuery = context.exp_data(nestedQuery, config)

        body = {
                'query': nestedQuery,
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _getNestedMasterItemScript(self, config, context):
        """
        DeleteEventProcessor
        """
        query = context.exp_data(config['query'], config)

        configList = config.getLocatedConfigList()
        parentField = configList.getParentField()

        inlineScript = self._getInlineScript(config)
        if not inlineScript:
            inlineTemplate = Template("""if (ctx._source.${field} == null) { ctx._source.${field} = []; } ctx._source.${field}.removeIf(item -> ${query_condition});""")

            queryConditions = []
            for key in query:
                queryConditions.append('item.%s == params.query.%s' % (key, key))

            inlineScript = inlineTemplate.substitute(
                    field=parentField,
                    query_condition='&&'.join(queryConditions)
                    ) 
            self._setInlineScript(config, inlineScript)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': {
                    'query': query
                    }
                }
        return script

    def _getSlaveItemScript(self, config, relativedConfigs, context):
        """
        DeleteEventProcessor
        """
        inlineScript = self._getInlineScript(config)
        if not inlineScript:
            inlineList = []
            for conf in relativedConfigs.values():
                mapping = conf['mapping']
                for item in mapping:
                    esField = item['es_field']
                    inlineList.append("ctx._source.%s = params.%s" % (esField, esField))

            inlineScript = ';'.join(inlineList) + ';'
            self._setInlineScript(config, inlineScript)

        params = {}
        for conf in relativedConfigs.values():
            confKey = conf['key']
            mapping = conf['mapping']
            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                itemType = item['type']
                evalOnDeleted = item['eval_on_deleted']
                nullValue = item['null_value']
        
                if evalOnDeleted:
                    values = context.getData(confKey)

                    if itemType == 'nested':
                        params[esField] = context.getNestedData(confKey, esField)
                    else:
                        params[esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)
                else:
                    params[esField] = nullValue

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

    def _getNestedSlaveItemScript(self, config, relativedConfigs, context):
        """
        DeleteEventProcessor
        """
        query = context.exp_data(config['query'], config)

        inlineScript = self._getInlineScript(config)
        if not inlineScript:
            inlineTemplate = Template("""
            for (item in ctx._source.${parent_field}) {
                if (${query_condition}) {
                    ${item_evaluation};
                }
            }
            """)

            queryConditions = []
            for key in query:
                queryConditions.append('item.%s == params.query.%s' % (key, key))

            itemEvaluations = []
            for conf in relativedConfigs.values():
                mapping = conf['mapping']
                for item in mapping:
                    esField = item['es_field']

                    # no nested type in nested config item
                    itemEvaluations.append("item.%s = params.data.%s" % (esField, esField))

            parentField = config.getLocatedConfigList().getParentField()
            inlineScript = inlineTemplate.substitute(
                    parent_field=parentField,
                    query_condition='&&'.join(queryConditions),
                    item_evaluation=';'.join(itemEvaluations)
                    )
            self._setInlineScript(config, inlineScript)

        params = {
                'query': query,
                'data': {}
                }
        for conf in relativedConfigs.values():
            confKey = conf['key']
            mapping = conf['mapping']
            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                evalOnDeleted = item['eval_on_deleted']
                nullValue = item['null_value']

                # no nested type in nested config item
                if evalOnDeleted:
                    values = context.getData(confKey)
                    params['data'][esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)
                else:
                    params['data'][esField] = nullValue

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

class UpdateEventProcessor(_BaseEventProcessor):
    def __init__(self, statusConfig, insertProcessor, deleteProcessor):
        self._insertProcessor = insertProcessor
        self._deleteProcessor = deleteProcessor

        super(UpdateEventProcessor, self).__init__(statusConfig)

    def _updateTotally(self, config, binlogEvent):
        database = binlogEvent['database']
        table = binlogEvent['table']
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        self._deleteProcessor.process(config, CommonUtils.buildBinlogEventLog(database, table, 'DELETE', beforeValues))
        self._insertProcessor.process(config, CommonUtils.buildBinlogEventLog(database, table, 'INSERT', afterValues))

    def _needUpdateTotally(self, config, fields):
        anchorFields = config.getAnchorFields()
        _logger.debug("fields: %s", fields)
        _logger.debug("anchorFields of config[%s]: %s", config, anchorFields)
        if anchorFields and fields:
            interset = anchorFields.intersection(set(fields))
            return bool(interset)

        return False
        
    def _processMasterItem(self, config, binlogEvent):
        """
        UpdateEventProcessor
        """
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        # 首先找出更新前后发生变化的字段集合，用来寻找依赖项
        fields = self._getDiffFields(beforeValues, afterValues)

        # 判断是否需要updateTotally
        if self._needUpdateTotally(config, fields):
            self._updateTotally(config, binlogEvent)
            return

        # 找出相关的依赖项，更新它们
        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, fields, withSelf=True)

        # 定义context
        context = HandlerContext(relativedConfigs, { configKey: afterValues })

        script = self._getMasterItemScript(config, fields, relativedConfigs, context)
        body = { 'script': script }

        documentId = context.exp_value(config['document_id'], config)
        routing = config.get('routing', None)
        if routing:
            routing = context.exp_value(config['routing'], config)

        self._esClient.update(
                index=self._getESIndexFullname(config.esIndex),
                doc_type=config.esType,
                id=documentId,
                routing=routing,
                body=body,
                retry_on_conflict=_MAX_RETRY_COUNT
                )

    def _processNestedMasterItem(self, config, binlogEvent):
        """
        UpdateEventProcessor
        """
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        # 首先找出更新前后发生变化的字段集合，用来寻找依赖项
        fields = self._getDiffFields(beforeValues, afterValues)

        # 判断是否需要updateTotally
        if self._needUpdateTotally(config, fields):
            self._updateTotally(config, binlogEvent)
            return

        # 找出相关的依赖项，更新它们
        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(configKey, fields, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: afterValues })

        script = self._getNestedMasterItemScript(config, fields, relativedConfigs, context)

        nestedQuery = self._getNestedBoolQuery(config)
        nestedQuery = context.exp_data(nestedQuery, config)

        body = {
                'query': nestedQuery,
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _processSlaveItem(self, config, binlogEvent):
        """
        UpdateEventProcessor
        """
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        # 首先找出更新前后发生变化的字段集合
        fields = self._getDiffFields(beforeValues, afterValues)

        # 判断是否需要updateTotally
        if self._needUpdateTotally(config, fields):
            self._updateTotally(config, binlogEvent)
            return

        # 找出相关的依赖项，更新它们
        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(config.key, fields, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: afterValues })

        script = self._getSlaveItemScript(config, fields, relativedConfigs, context)

        query = context.exp_data(config['query'], config)

        body = {
                'query': self._getBoolQuery(query),
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _processNestedSlaveItem(self, config, binlogEvent):
        """
        UpdateEventProcessor
        """
        beforeValues = binlogEvent['before']
        afterValues = binlogEvent['values']

        # 首先找出更新前后发生变化的字段集合
        fields = self._getDiffFields(beforeValues, afterValues)

        # 判断是否需要updateTotally
        if self._needUpdateTotally(config, fields):
            self._updateTotally(config, binlogEvent)
            return

        # 找出相关的依赖项，更新它们
        configKey = config.key
        configList = config.getLocatedConfigList()
        relativedConfigs = configList.getDependentItems(config.key, fields, withSelf=True)
        context = HandlerContext(relativedConfigs, { configKey: afterValues })

        script = self._getNestedSlaveItemScript(config, fields, relativedConfigs, context)

        nestedQuery = self._getNestedBoolQuery(config)
        nestedQuery = context.exp_data(nestedQuery, config)

        body = {
                'query': nestedQuery,
                'script': script
                }

        self._updateByQuery(config.esIndex, config.esType, body)

    def _getMasterItemScript(self, config, fields, relativedConfigs, context):
        """
        UpdateEventProcessor
        """
        dependentNestedLists = config.getNestedDependentLists(fields=fields)

        inlineScript = self._getInlineScript(config, extraKeys=fields)
        if not inlineScript:
            inlineList = []
            for conf in relativedConfigs.values():
                mapping = conf['mapping']
                for item in mapping:
                    esField = item['es_field']
                    itemType = item['type']

                    if conf is config and itemType == 'nested':
                        nestedList = conf.getNestedListByEsField(esField)
                        if nestedList in dependentNestedLists:
                            inlineList.append("ctx._source.%s = params.%s" % (esField, esField))
                    else:
                        inlineList.append("ctx._source.%s = params.%s" % (esField, esField))

            inlineScript = ';'.join(inlineList) + ';'
            self._setInlineScript(config, inlineScript, extraKeys=fields)

        params = {}
        for conf in relativedConfigs.values():
            confKey = conf['key']
            mapping = conf['mapping']
            values = context.getData(confKey)
            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                itemType = item['type']
                nullValue = item['null_value']
        
                if itemType == 'nested':
                    if conf is config:
                        nestedList = conf.getNestedListByEsField(esField)
                        if nestedList in dependentNestedLists:
                            params[esField] = context.getNestedData(confKey, esField)
                    else:
                        params[esField] = context.getNestedData(confKey, esField)
                else:
                    params[esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

    def _getNestedMasterItemScript(self, config, fields, relativedConfigs, context):
        """
        UpdateEventProcessor
        """
        query = context.exp_data(config['query'], config)

        inlineScript = self._getInlineScript(config, extraKeys=fields)
        if not inlineScript:
            inlineTemplate = Template("""
            for (item in ctx._source.${parent_field}) {
                if (${query_condition}) {
                    ${item_evaluation};
                }
            }
            """)

            queryConditions = []
            for key in query:
                queryConditions.append('item.%s == params.query.%s' % (key, key))

            itemEvaluations = []
            for conf in relativedConfigs.values():
                mapping = conf['mapping']
                for item in mapping:
                    esField = item['es_field']

                    # no nested type in nested config item
                    itemEvaluations.append("item.%s = params.data.%s" % (esField, esField))

            parentField = config.getLocatedConfigList().getParentField()
            inlineScript = inlineTemplate.substitute(
                    parent_field=parentField,
                    query_condition='&&'.join(queryConditions),
                    item_evaluation=';'.join(itemEvaluations)
                    )
            self._setInlineScript(config, inlineScript, extraKeys=fields)

        params = {
                'query': query,
                'data': {}
                }
        for conf in relativedConfigs.values():
            confKey = conf['key']
            mapping = conf['mapping']
            values = context.getData(confKey)
            for item in mapping:
                esField = item['es_field']
                dbField = item['db_field']
                nullValue = item['null_value']
        
                # no nested type in nested config item
                params['data'][esField] = CommonUtils.getDBFieldValue(dbField, values, conf, nullValue)

        script = {
                'lang': 'painless', 
                'inline': inlineScript,
                'params': params
                }
        return script

    def _getSlaveItemScript(self, config, fields, relativedConfigs, context):
        """
        UpdateEventProcessor
        """
        return self._getMasterItemScript(config, fields, relativedConfigs, context)

    def _getNestedSlaveItemScript(self, config, dependentConfigs, context, fields):
        """
        UpdateEventProcessor
        """
        return self._getNestedMasterItemScript(config, dependentConfigs, context, fields)

class CommonUtils(object):
    @staticmethod
    def getDBFieldValue(dbField, values, config, nullValue=None):
        if not values or not dbField:
            return nullValue

        kwargs = {
                'values': values,
                'index': config.esIndex,
                'type': config.esType,
                'database': config['database'],
                'table': config['table']
                }

        retValue = common.resolve(dbField, **kwargs)
        if retValue is None:
            return nullValue
        else:
            return retValue

    @staticmethod
    def buildBinlogEventLog(database, table, eventType, values):
        return {
                'database': database,
                'table': table,
                'type': eventType,
                'values': values
                }

    @staticmethod
    def filterData(filterDict, values):
        """
        是否过滤指定数据
        True 数据通过
        False 数据被过滤
        """
        _logger.debug('filterDict[%s] ; values[%s]', filterDict, values)
        if filterDict:
            for filterKey in filterDict.keys():
                if filterKey not in values:
                    return False

                filterValue = filterDict[filterKey]
                dataValue = values[filterKey]
                if isinstance(filterValue, list):
                    if dataValue not in filterValue:
                        return False
                elif isinstance(filterValue, dict):
                    for op in filterValue.keys():
                        value = filterValue[op]
                        if op == '==':
                            if dataValue != value:
                                return False
                        elif op == '!=' or op == '<>':
                            if dataValue == value:
                                return False
                        elif op == '>':
                            if dataValue <= value:
                                return False
                        elif op == '>=':
                            if dataValue < value:
                                return False
                        elif op == '<':
                            if dataValue >= value:
                                return False
                        elif op == '<=':
                            if dataValue > value:
                                return False
                        else:
                            raise IllegalConfigException('filter op NOT supported yet: %s' % op)
                else:
                    if dataValue != filterValue:
                        return False

        # 默认通过
        return True

