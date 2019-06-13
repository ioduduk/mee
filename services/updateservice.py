# -*- coding: utf-8 -*-

from __future__ import print_function, division

import time
import sys
import os
import random

import utils
import application.app as app
import modules.remote as remote

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.client import IndicesClient

from confluent_kafka import TopicPartition
from application.config import config
from modules.status import RedisStatus, RedisStatusConfig, HandlerConfig
from modules.interfaces import (STATUS_INITIAL, 
                              STATUS_FULL_UPDATING,
                              STATUS_INCRE_UPDATING
                              )
from utils.failure import Failure

_logger = app.getLogger('base')
_DISTRIBUTE_REDIS_LOCK_PREFIX = '__mee_status_lock_'

class UpdateService(object):
    """
    全量更新的service
    """
    def __init__(self, taskName, handlerConfigPath):
        self.handlerConfigPath = handlerConfigPath

        self.redisClient = remote.getRedisClient()
        self.esClient = remote.getElasticClient()
        self.esIndexClient = IndicesClient(self.esClient)

        # instance of RedisStatus
        self.statusKey = taskName
        self.status = None

        # classname of handler
        self.handlerClassName = 'modules.handlers.v1.CommonHandler'

    def update(self):
        """
        update action.
        """
        # 检查status，看是否可以进行全量更新
        if not self._checkStatus():
            return False

        # 更新status，标明准备开始全量更新
        self._switchStatus()

        # 将新的消费者的offset设置为 latest
        self._copyKafkaOffset()

        # 从mysql读取数据，同步到ES中
        syncResult = self._syncFromMysqlToEs()

        if syncResult:
            # 从kafka队列中消费数据，直至赶上老的消费者 
            self._catchupwithCurrentConsumer()

            # 移除老的es index的alias，增加新的es index的alias
            self._setESIndexAlias()

            # 修改status
            self._resetStatus(True)
        else:
            # 修改status
            self._resetStatus(False)

        # 清理脏数据
        self._cleanDirtyData()
        
        return syncResult

    def reset(self):
        """
        重新设置status，清除脏数据
        """
        status = RedisStatus(self.statusKey)
        status.sync()
        self.status = status

        self._restoreESIndexAlias()
        self._resetStatus(False)
        self._cleanDirtyData()

    def clean(self):
        """
        清除脏数据
        """
        self._cleanDirtyData()

    def _cleanDirtyData(self):
        """
        清除发生异常时导致的脏数据。
        包括：无用的statusconfig, 无用的es index
        """
        configKeys = self.redisClient.keys('CK_%s_*' % self.statusKey)
        status = RedisStatus(self.statusKey)
        status.sync()

        liveKeys = [status.config, status.nextConfig, status.tmpConfig]
        for key in configKeys:
            if key not in liveKeys:
                self._deleteStatusConfig(key, deleteIndices=True)
            elif key == status.tmpConfig:
                self._deleteStatusConfig(key, deleteIndices=False, deleteAlias=True)

    def _fail(self, errinfo):
        _logger.error(errinfo)
        print(errinfo, file=sys.stderr)

    def _checkStatus(self):
        status = RedisStatus(self.statusKey)
        status.sync()

        self.status = status

        if status.code != STATUS_INITIAL and status.code != STATUS_INCRE_UPDATING:
            self._fail('status code [%s] MUST be in values array [%s, %s]' % (status, STATUS_INITIAL, STATUS_INCRE_UPDATING))
            return False
        else:
            return True

    def _loadConfig(self):
        """
        从conf/handlers/目录中加载handlers相关的配置，
        此外，还会生成一个新的consumer_group_id以及一个新的es_index,
        这些数据都写入redis
        """
        configKey = 'CK_%s_%s' % (self.statusKey, app.getUuid('update.loadconfig.gen.config_key'))

        kafkaGroupId = app.getUuid('update.loadconfig.gen.kafka_group_id')

        esIndexSuffix = str(int(time.time())) + '_' + str(random.randint(0, 100000))

        handlerConfig = HandlerConfig()
        handlerConfig.loadFromFile(self.handlerConfigPath)

        statusConfig = RedisStatusConfig(configKey)
        statusConfig.kafkaGroupId = kafkaGroupId
        statusConfig.esIndexSuffix = esIndexSuffix
        statusConfig.handlerConfig = handlerConfig
        
        statusConfig.set()

        _logger.info('load config: %s', statusConfig)
        
        return statusConfig

    def _acquireDLock(self):
        return self.redisClient.acquireDLock(_DISTRIBUTE_REDIS_LOCK_PREFIX + self.statusKey)

    def _releaseDLock(self, lock):
        return self.redisClient.releaseDLock(lock)

    def _switchStatus(self):
        """
        更新status，标明准备开始全量更新
        """
        lock = None
        try:
            # 创建全量更新时所需的statusConfig
            statusConfig = self._loadConfig()

            lock = self._acquireDLock()

            # check the stauts again after get the lock
            if not self._checkStatus():
                return False
            
            self.status.code = STATUS_FULL_UPDATING
            self.status.nextConfig = statusConfig.key

            self.status.update()
        except Exception as e:
            _logger.info('fail to switch status to full updating: %s', e)
            raise
        finally:
            if lock:
                self._releaseDLock(lock)

    def _copyKafkaOffset(self):
        """
        将新的消费者的offset设置为 latest
        """
        # 首先要获取kafka topic的所有分区
        topicName = config().get('kafka', 'topic')

        if self.status.nextConfig:
            nextStatusConfig = RedisStatusConfig(self.status.nextConfig, forceSync=True) 

            try:
                nextConsumer = remote.getKafkaConsumer(
                        nextStatusConfig.kafkaGroupId,
                        autoCommit=False,
                        autoOffsetReset='latest'
                        )
                
                _logger.debug('next kafka groupid is: %s', nextStatusConfig.kafkaGroupId)

                clusterMetadata = nextConsumer.list_topics(topicName)
                topicMetadata = clusterMetadata.topics.get(topicName, {})
                partitions = topicMetadata.partitions

                for pid in partitions.keys():
                    p = TopicPartition(topicName, pid)
                    nextConsumer.assign([p])

                    msg = nextConsumer.poll(10)
                    if msg:
                        offset = msg.offset() - 1
                        _logger.debug('pid[%s] topic[%s] offset[%s]', pid, topicName, offset)

                        if offset >= 0:
                            p.offset = offset
                            nextConsumer.commit(offsets=[p])
            except Exception as e:
                _logger.error('exception occurs when setting offset for new consumer: %s', Failure()) 
                raise
            finally:
                if nextConsumer:
                    nextConsumer.close()
                
    def _syncFromMysqlToEs(self):
        nextStatusConfig = RedisStatusConfig(self.status.nextConfig, forceSync=True) 

        # 从mysql中全量更新
        try:
            handlerClass = utils.classForName(self.handlerClassName)
            commonHandler = handlerClass(nextStatusConfig)
            commonHandler.syncFromMySQL()
            return True
        except Exception as e:
            _logger.error('exception occurs when syncFromMySQL: %s', Failure())
            return False

    def _catchupwithCurrentConsumer(self):
        """
        取新的consumer_group_id，从kafka队列中消费数据。
        直到新的消费者赶上旧的消费者。
        """
        # TODO

    def _setESIndexAlias(self):
        """
        移除老的es index的alias，增加新的es index的alias
        """
        configKey = self.status.config
        if configKey:
            statusConfig = RedisStatusConfig(configKey, forceSync=True)
            self._removeESIndexAlias(statusConfig)

        nextConfigKey = self.status.nextConfig
        if nextConfigKey:
            nextStatusConfig = RedisStatusConfig(nextConfigKey, forceSync=True)
            self._addESIndexAlias(nextStatusConfig)

    def _restoreESIndexAlias(self):
        """
        恢复老的es index的alias，删除新的es index的alias
        """
        nextConfigKey = self.status.nextConfig
        if nextConfigKey:
            nextStatusConfig = RedisStatusConfig(nextConfigKey, forceSync=True)
            self._removeESIndexAlias(nextStatusConfig)

        configKey = self.status.config
        if configKey:
            statusConfig = RedisStatusConfig(configKey, forceSync=True)
            self._addESIndexAlias(statusConfig)

    def _removeESIndexAlias(self, statusConfig):
        """
        移除指定es index的alias
        """
        esIndexSuffix = statusConfig.esIndexSuffix
        indices = statusConfig.handlerConfig.indices()
        for indexAlias in indices:
            indexName = indexAlias + '_' + esIndexSuffix
            try:
                self.esIndexClient.delete_alias(
                        index=indexName,
                        name=indexAlias
                        )
            except NotFoundError as e:
                _logger.error('index[%s] not found: %s', indexName, e)

    def _addESIndexAlias(self, statusConfig):
        """
        移除指定es index的alias
        """
        esIndexSuffix = statusConfig.esIndexSuffix
        indices = statusConfig.handlerConfig.indices()
        for indexAlias in indices:
            indexName = indexAlias + '_' + esIndexSuffix
            try:
                self.esIndexClient.put_alias(
                        index=indexName,
                        name=indexAlias
                        )
            except NotFoundError as e:
                _logger.error('index[%s] not found: %s', indexName, e)


    def _resetStatus(self, succeeded):
        """
        重置status，标明全量更新完毕，开始进行增量更新
        """
        try:
            lock = self._acquireDLock()

            if succeeded:
                # after succeeded
                self.status.code = STATUS_INCRE_UPDATING
                self.status.tmpConfig = self.status.config
                self.status.config = self.status.nextConfig
                self.status.nextConfig = ''

                self.status.update()
            else:
                # after failed
                self.status.code = STATUS_INCRE_UPDATING
                self.status.tmpConfig = self.status.nextConfig
                self.status.nextConfig = ''

                self.status.update()

                # if failed, delete dirty data immediately
                self._deleteStatusConfig(self.status.tmpConfig, deleteIndices=True)

        except Exception as e:
            _logger.info('fail to reset status to incre-updating: %s', Failure())
            raise
        finally:
            if lock:
                self._releaseDLock(lock)

    def _deleteStatusConfig(self, configKey, deleteIndices=False, deleteAlias=False):
        # 删除stats config
        if configKey:
            statusConfig = RedisStatusConfig(configKey, forceSync=True)

            if deleteIndices:
                handlerConfig = statusConfig.handlerConfig
                for indexAlias in handlerConfig.indices():
                    indexName = indexAlias + '_' + statusConfig.esIndexSuffix

                    if self.esIndexClient.exists(index=indexName):
                        self.esIndexClient.delete(
                                index=indexName
                                )
            elif deleteAlias:
                # 只删除alias，不删除index
                self._removeESIndexAlias(statusConfig)

            # 在redis中删除status config
            # 设置为1天后过期
            statusConfig.delete(24 * 3600)

    def _removeNextEsIndex(self):
        """
        删除新的es index
        """
        # TODO

    def _removeNextStatusConfig(self):
        """
        删除新的es index
        """
        # TODO













