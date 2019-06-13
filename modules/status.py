# -*- coding: utf-8 -*-

from __future__ import print_function, division

import time

import modules.remote as remote
import application.app as app

from zope.interface import implementer

from application import IllegalArgumentException, LogicException
from utils.singleton import singleton, cache
from .interfaces import *
from .handlers.handlerconfig import *

_logger = app.getLogger('base')

@implementer(IStatus)
class RedisStatus(object):
    _STATUS_KEY_PREFIX = '__mee_status_'

    def __init__(self, key, code='', config='', nextConfig='', tmpConfig=''):
        self.key = self._STATUS_KEY_PREFIX + key

        self.code = code
        self.config = config
        self.nextConfig = nextConfig
        self.tmpConfig = tmpConfig

        self.redisclient = remote.getRedisClient()

    def sync(self):
        """
        从远程服务器同步数据，如果数据发生了改变，则会返回True; 否则返回False
        """
        syncResult = self.redisclient.hgetall(self.key)
        if syncResult is None:
            syncResult = {}
            
        # if no status on redis, means status is initial
        code = syncResult.get('code', STATUS_INITIAL)
        config = syncResult.get('config', '')
        nextConfig = syncResult.get('next_config', '')
        tmpConfig = syncResult.get('tmp_config', '')

        noChanged = (
                code == self.code 
                and config == self.config
                and nextConfig == self.nextConfig
                and tmpConfig == self.tmpConfig
                )

        self.code = code
        self.config = config
        self.nextConfig = nextConfig
        self.tmpConfig = tmpConfig

        return False if noChanged else True

    def create(self):
        return self.update()

    def update(self):
        result = self.redisclient.hmset(self.key, self.toDict())
        return result

    def delete(self):
        result = self.redisclient.delete(self.key)
        return result

    def toDict(self):
        return {
                'code': self.code,
                'config': self.config,
                'next_config': self.nextConfig,
                'tmp_config': self.tmpConfig
                } 

    def __str__(self):
        dataDict = self.toDict()
        dataDict['key'] = self.key
        return str(dataDict)

    def __eq__(self, otherStatus):
        if not isinstance(otherStatus, RedisStatus):
            raise IllegalArgumentException('otherStatus must be an instance of RedisStatus')

        return (
                self.key == otherStatus.key
                and self.code == otherStatus.code 
                and self.config == otherStatus.config
                and self.nextConfig == otherStatus.nextConfig
                and self.tmpConfig == otherStatus.tmpConfig
                )

    @staticmethod
    def fromDict(key, dictData):
        return RedisStatus(key, **dictData)

@implementer(IStatusConfig)
class RedisStatusConfig(object):
    """
    """

    __metaclass__ = cache

    def __init__(self, key, forceSync=False):
        """
        """
        self.key = key
        self.redisclient = remote.getRedisClient()

        if forceSync:
            self.syncByKey()
        else:
            self.init()

    def syncByKey(self):
        if not self.key:
            raise LogicException('key of statusconfig CANNOT be empty')

        syncResult = self.redisclient.hgetall(self.key)

        self.kafkaGroupId = syncResult.get('kafka_group_id', '')
        self.esIndexSuffix = syncResult.get('es_index_suffix', '')

        handlerJsonStr = syncResult.get('handler_config', '{}')
        try:
            if not getattr(self, 'handlerConfig', None):
                self.handlerConfig = HandlerConfig()

            self.handlerConfig.loadFromJson(handlerJsonStr)
        except Exception as e:
            _logger.error('fail to loads handler config[%s]: %s', self.key, e)
            raise LogicException('the handler config string MUST be jsonable: %s' % handlerJsonStr)

        if syncResult:
            self.synced = True
        else:
            self.synced = False

        return self

    def set(self):
        if not self.key:
            raise LogicException('key of statusconfig CANNOT be empty')

        result = self.redisclient.hmset(self.key, self.toDict())
        if result:
            self.synced = True

        return result

    def init(self):
        self.kafkaGroupId = ''
        self.esIndexSuffix = ''
        self.handlerConfig = HandlerConfig()
        self.synced = False

    def delete(self, expired=0):
        if not self.key:
            raise LogicException('key of statusconfig CANNOT be empty')

        if not expired:
            result = self.redisclient.delete(self.key)
            self.init()
        else:
            result = self.redisclient.expire(self.key, expired)

        return result

    def toDict(self):
        return {
                'key': self.key,
                'kafka_group_id': self.kafkaGroupId,
                'es_index_suffix': self.esIndexSuffix,
                'handler_config': self.handlerConfig.toJson()
                } 

    def toSimpleString(self):
        data = {
                'key': self.key,
                'kafka_group_id': self.kafkaGroupId,
                'es_index_suffix': self.esIndexSuffix,
                'synced': self.synced
                }
        return str(data)


    def __str__(self):
        data = self.toDict()
        data['synced'] = self.synced
        return str(data)

