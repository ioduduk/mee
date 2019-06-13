# -*- coding: utf-8 -*-

from __future__ import print_function, division

import yaml
import re
import copy
import simplejson as json

from collections import MutableMapping
from zope.interface import implementer

import application.app as app

from application import IllegalConfigException, IllegalArgumentException, LogicException
from utils.failure import Failure
from .loader import Loader
from ..handlers import *
from ..interfaces import *

_initCount = 0
_loadCount = 0
_logger = app.getLogger('base')

_DEPENDENCE_RE = re.compile(r'(%%)|%(?:(\w+)\.)?(\w+)')
_VALID_MAPPING_ES_FIELD = re.compile(r'^[_a-zA-Z]\w*$')

"""
"""
_SQL_STATEMENT_LIMIT_RE = re.compile(r'\s+limit\s+\d+\s*(,\s*\d+)?\s*$', re.I)

@implementer(IHandlerConfig)
class HandlerConfig(object):
    def __init__(self):
        self._data = {}
        self._forward = {}

        global _initCount
        _initCount += 1
        _logger.debug('HandlerConfig loaded count: %s, %s', _initCount, _loadCount)

    def loadFromJson(self, jsonData):
        try:
            self._data = json.loads(jsonData)
        except Exception as e:
            _logger.error('fail to loads handler config from json data: %s', e)
            raise IllegalConfigException('the jsonData string MUST be jsonable: %s' % Failure())

        self._resolve()

        global _loadCount
        _loadCount += 1
        _logger.debug('HandlerConfig loaded count: %s, %s', _initCount, _loadCount)

    def loadFromFile(self, filepath):
        try:
            loadData = yaml.load(open(filepath), Loader)

            if isinstance(loadData, list):
                data = {}
                for item in loadData:
                    data.update(item)
            else:
                data = loadData

            for key in data.keys():
                if key.startswith('__'):
                    del data[key]

            self._data = data
        except Exception as e:
            _logger.error('fail to loads handler config from file[%s]: %s', filepath, e)
            raise IllegalConfigException('the config file[%s] MUST be valid: %s' % (filepath, Failure()))
        
        self._resolve()

        global _loadCount
        _loadCount += 1
        _logger.debug('HandlerConfig loaded count: %s, %s', _initCount, _loadCount)

    def _resolve(self):
        for index in self._data.keys():
            if index not in self._forward:
                self._forward[index] = {}

            types = self._data[index].keys()
            for esType in types:
                items = copy.deepcopy(self._data[index][esType])
                configList = HandlerConfigList(index, esType, items)
                self._forward[index][esType] = configList

    def indices(self):
        return self._forward.keys()

    def types(self, index):
        return self._forward.get(index, {}).keys()
    
    def getConfigListByIndexAndType(self, index, esType):
        data = self._forward.get(index, None)
        if data:
            return data.get(esType, None)
        else:
            return None

    def getConfigItemsByDatabaseAndTable(self, database, table):
        items = []
        for configList in self:
            items += configList.getItemsByDatabaseAndTable(database, table)

        return items

    def toJson(self):
        return json.dumps(self._data)

    def __iter__(self):
        for index in self._forward:
            for esType in self._forward[index]:
                yield self._forward[index][esType]

@implementer(IHandlerConfigList)
class HandlerConfigList(object):
    def __init__(self, esIndex, esType, items):
        self.esIndex = esIndex
        self.esType = esType
        self._masterItem = None
        self._slaveItems = []
        self._allItems = []
        self._inverted = {}
        self._nestedLists = {}

        self._load(items)

    def getMasterItem(self):
        return self._masterItem

    def getMasterKey(self):
        return self._masterItem['key']

    def getAllItems(self):
        return self._allItems

    def getSlaveItems(self):
        return self._slaveItems

    def getItemsByDatabaseAndTable(self, database, table):
        items = self._inverted.get(database, {}).get(table, [])
        nestedItems = []
        for nestedList in self._nestedLists.values():
            nestedItems += nestedList.getItemsByDatabaseAndTable(database, table)

        return items + nestedItems

    def getConfigItemByKey(self, key):
        for item in self:
            if item['key'] == key:
                return item
    
        return None

    def addNestedConfigList(self, esField, nestedList):
        if esField in self._nestedLists:
            raise IllegalConfigException('es_field[%s] of nested duplicated' % esField)
        
        self._nestedLists[esField] = nestedList

    def isNested(self):
        return False

    def _load(self, items):
        """
        首先解析mapping
        然后解析出各个配置之间的依赖关系，同时尝试检查是否有循环依赖
        """
        index = self.esIndex
        esType = self.esType
        keys = set()
        for data in items:
            key = data['key']
            database = data['database']
            table = data['table']
            if key in keys:
                raise IllegalConfigException('Duplicated key[%s] detected in [%s, %s]' % (key, index, esType))

            keys.add(key)

            configItem = HandlerConfigItem(self, data)
            self._allItems.append(configItem)

            if database not in self._inverted:
                self._inverted[database] = {}
            if table not in self._inverted[database]:
                self._inverted[database][table] = []
            self._inverted[database][table].append(configItem)

            if configItem.isMaster:
                if self._masterItem:
                    raise IllegalConfigException('Duplicated master item detected in [%s, %s]' % (index, esType))
                self._masterItem = configItem
            else:
                self._slaveItems.append(configItem)

        # 检查是否存在master item
        if not self._masterItem:
            raise IllegalConfigException('master item NOT found in [%s, %s]' % (index, esType))

        # 生成依赖关系
        self._resolveDependences()

        # 检查依赖关系是否合法
        self._checkValidDependences()

        # 检查是否存在循环依赖
        loopDedected = self._checkLoopDependences()
        if loopDedected:
            raise IllegalConfigException('Loop Dependence detected in [%s, %s]' % (index, esType))

    def __iter__(self):
        return iter(self._allItems)

    def _resolveDependences(self):
        self._dependences = {}
        self._dependents = {}
        for item in self:
            self._resolveDependenceByItem(item)

    def _resolveDependenceByItem(self, item):
        key = item['key']
        statement = item['statement']

        depends = _DEPENDENCE_RE.findall(statement)

        dependKeys = set()
        for dep in depends:
            dKey = dep[1]
            dField = dep[2]

            if not dKey or dKey == key:
                continue
            elif dKey == '__master':
                dKey = self._masterItem['key']

            if dKey not in self._dependents:
                self._dependents[dKey] = {}

            if dField not in self._dependents[dKey]:
                self._dependents[dKey][dField] = set()

            self._dependents[dKey][dField].add(key)

            if dKey not in ('__last', '__parent'):
                dependKeys.add(dKey)

        if len(dependKeys) > 1:
            raise IllegalConfigException('dependence count can NOT be greater than one: esIndex[%s], esType[%s], key[%s]' % (item.esIndex, item.esType, key))

    def _checkValidDependences(self):
        """
        检查依赖关系是否合法
        """
        # __last 依赖，只能出现在master item/nested master item
        lastDependents = self._getDirectDependentKeys('__last')
        for key in lastDependents:
            config = self.getConfigItemByKey(key)
            if not config.isMaster:
                raise IllegalConfigException('__last dependence can ONLY appear in master config item, not current item[%s]' % key)

        # __parent 依赖，只能出现在nested master item
        parentDependents = self._getDirectDependentKeys('__parent')
        for key in parentDependents:
            config = self.getConfigItemByKey(key)
            if not config.isMaster or not config.isNested():
                raise IllegalConfigException('__parent dependence can ONLY appear in nested master config item, not current item[%s]' % key)

    def _checkLoopDependences(self):
        masterKey = self._masterItem['key']
        return self._checkLoopDependenceByKey(masterKey, set(masterKey))

    def _checkLoopDependenceByKey(self, key, touchedSet):
        """
        深度优先检查是否出现了循环依赖
        """
        data = self._dependents.get(key, None)
        if not data:
            return False

        dependSet = set()
        for values in data.values():
            dependSet.update(values)

        for item in dependSet:
            if item in touchedSet:
                return True

            touchedSet.add(item)

            if self._checkLoopDependenceByKey(item, touchedSet):
                return True
            
            touchedSet.remove(item)

        return False

    def getDependentItems(self, key, fields=None, withSelf=False):
        dependKeys = self._getDependentKeys(key, set(), fields=fields)

        if withSelf:
            dependKeys.add(key)

        result = { item['key']: item for item in self if item['key'] in dependKeys }
        return result

    def _getDirectDependentKeys(self, key, fields=None):
        chain = set()

        data = self._dependents.get(key, None)
        if not data:
            return chain

        if fields:
            for field in fields:
                keys = data.get(field, [])
                chain.update(keys)
        else:
            for values in data.values():
                chain.update(values)

        return chain

    def _getDependentKeys(self, key, touchedSet, fields=None):
        """
        广度优先获取所有的被依赖项
        """
        chain = set()

        if key in touchedSet:
            return chain

        chain = self._getDirectDependentKeys(key, fields=fields)

        touchedSet.add(key)
        
        for item in list(chain): # need list here, otherwise chain will change at iteration
            if item not in touchedSet:
                tmpResult = self._getDependentKeys(item, touchedSet)
                chain.update(tmpResult)

        return chain

class NestedHandlerConfigList(HandlerConfigList):
    def __init__(self, parentItem, parentField, items):
        self._parentItem = parentItem
        self._parentField = parentField

        esIndex = parentItem.esIndex
        esType = parentItem.esType
        super(NestedHandlerConfigList, self).__init__(esIndex, esType, items)

    def isNested(self):
        return True

    def getParentItem(self):
        return self._parentItem

    def getParentKey(self):
        return self._parentItem.key

    def getParentField(self):
        return self._parentField

    def getParentDependents(self):
        return self._dependents.get('__parent', {})

    def addNestedConfigList(self, esField, nestedList):
        raise IllegalConfigException('NestedHandlerConfigList must NOT contain any child NestedHandlerConfigList: %s, %s' % (self.esIndex, self.esType))

@implementer(IHandlerConfigItem)
class HandlerConfigItem(MutableMapping):
    def __init__(self, locatedList, data):
        self._locatedList = locatedList
        self._data = data if data else {}

        self.key = self._data.get('key', None)
        self.esIndex = locatedList.esIndex
        self.esType = locatedList.esType

        self.isMaster = data.has_key('document_id')

        self._nestedLists = {}

        self._nestedDependents = {}

        self._resolveMapping()

        self._checkValidation()

        self._computeAnchorFields()

    def _resolveMapping(self):
        mapping = self._data['mapping']
        for index, mapItem in enumerate(mapping):
            if isinstance(mapItem, dict):
                if 'field' in mapItem:
                    mapItem['db_field'] = mapItem['field']
                    mapItem['es_field'] = mapItem['field']
                    del mapItem['field']

                if 'type' not in mapItem:
                    mapItem['type'] = 'default'

                if 'eval_on_deleted' not in mapItem:
                    mapItem['eval_on_deleted'] = False
                else:
                    mapItem['eval_on_deleted'] = bool(mapItem['eval_on_deleted'])

                if 'null_value' not in mapItem:
                    mapItem['null_value'] = None

                if mapItem['type'] == 'nested':
                    esField = mapItem['es_field']

                    nestedConfigList = NestedHandlerConfigList(self, esField, mapItem['db_field'])
                    mapItem['db_field'] = nestedConfigList

                    self._locatedList.addNestedConfigList(esField, nestedConfigList)

                    for field in nestedConfigList.getParentDependents():
                        self._nestedDependents[field] = nestedConfigList
                    
                    self._nestedLists[esField] = nestedConfigList
            elif isinstance(mapItem, basestring):
                self._data['mapping'][index] = {
                        'db_field': mapItem,
                        'es_field': mapItem,
                        'type': 'default',
                        'eval_on_deleted': False,
                        'null_value': None
                        }
            else:
                raise IllegalConfigException('mapping values MUST be dict or string: %s' % mapItem)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        self._data[key] = value

    def __delitem__(self, key):
        if key in self._data:
            del self._data[key]
            
    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __str__(self):
        return str({
            'key': self.key,
            'esIndex': self.esIndex,
            'esType': self.esType,
            'data': self._data
            })

    def __repr__(self):
        return self.__str__()

    def getLocatedConfigList(self):
        return self._locatedList

    def isNested(self):
        return self._locatedList.isNested()

    def hasNested(self):
        return bool(self._nestedLists)

    def getNestedListByEsField(self, esField):
        return self._nestedLists.get(esField, None)

    def getNestedDependentLists(self, fields=None):
        if not self._nestedDependents:
            return []

        if not fields:
            return self._nestedDependents.values()

        result = []
        for field in fields:
            tmp = self._nestedDependents.get(field, None)
            if tmp:
                result.append(tmp)

        return result

    def _checkValidation(self):
        # config.key必须存在
        if not self.key:
            raise IllegalConfigException('key of config item can NOT be empty: %s' % self)

        # mapping中的es_field必须是一个合法的标识符
        mapping = self._data['mapping']
        for item in mapping:
            esField = item['es_field']
            if not _VALID_MAPPING_ES_FIELD.match(esField):
                raise IllegalConfigException('es_field[%s] must be a valid identifier: %s' % (esField, self))

        # parent_query只能出现在 nested master item
        if not (self.isMaster and self.isNested()) and 'parent_query' in self._data:
            raise IllegalConfigException('parent_query can ONLY appear in nested master item: %s' % self)

        # 非master的item，以及nested中的所有item必须存在query
        # routing不允许出现在nested以及非master的item中
        if not self.isMaster or self.isNested():
            if 'query' not in self._data:
                raise IllegalConfigException('query property must EXIST in non-master/nesetd config item: %s' % self)

            if 'routing' in self._data:
                raise IllegalConfigException('routing property can NOT EXIST in non-master/nesetd config item: %s' % self)

        # statment中不能带有limit子句
        statement = self._data['statement']
        if _SQL_STATEMENT_LIMIT_RE.search(statement) is not None:
            raise IllegalConfigException('statement can NOT contain limit clause: [%s]' % statement)

    def getAnchorFields(self):
        return self._anchorFields

    def _computeAnchorFields(self):
        """
        通过分析document_id, routing, query, parent_query等属性，
        推导出当前config item的确定性字段：
        当这些字段的值发生改变时，整个config所对应的document要随之删除后再插入
        """
        self._anchorFields = set()

        for attr in ['document_id', 'routing', 'query', 'parent_query']:
            value = self._data.get(attr, None)
            if not value:
                continue

            self._getDependenceFields(value)

    def _getDependenceFields(self, value):
        if not value:
            return

        if isinstance(value, list):
            for item in value:
                self._getDependenceFields(item)
        elif isinstance(value, dict):
            for item in value.values():
                self._getDependenceFields(item)
        elif isinstance(value, basestring):
            depends = _DEPENDENCE_RE.findall(value)
            for dep in depends:
                percent = dep[0]
                key = dep[1]
                field = dep[2]

                if percent:
                    # met '%%'
                    continue

                if key and key != self.key:
                    # document_id, routing, query, parent_query只允许依赖当前的key
                    raise IllegalConfigException('anchor field[%s, %s] can NOT depend on other config item: %s' % (key, field, self))
                    
                self._anchorFields.add(field)
        else:
            pass # do nothing, just return 
