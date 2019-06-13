# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import os
import time
import simplejson as json

import application.app as app
import modules.remote as remote

from application.config import config
from modules.status import RedisStatus, RedisStatusConfig, HandlerConfig
from utils.singleton import cache
from utils.failure import Failure
from modules.handlers.v1 import CommonHandler

from .basecosumerservice import *

_logger = app.getLogger('base')

class SyncService(BaseConsumerService):
    """
    增量更新的service
    """

    _PATCH_MESSAGE_COUNT = 100

    def __init__(self, taskName):
        # instance of RedisStatus
        self.status = RedisStatus(taskName)
        self.relayTopic = config().get('kafka', 'topic')

        self.statusConfig = None
        self.kafkaConsumer = None
        self.handler = None

        self.itemCount = 0

        super(SyncService, self).__init__()

    def _reset(self):
        if self.kafkaConsumer:
            self.kafkaConsumer.close()

        self.statusConfig = None
        self.kafkaConsumer = None
        self.handler = None

        self.itemCount = 0

    def sync(self):
        while True:
            try:
                self._syncInternal()
            except Exception as e:
                _logger.error('exception occurs when sync data to es: %s', Failure())
                time.sleep(3)

                # something wrong on server??? suicide to restart (by supervisord)
                sys.exit(1)

    def _syncInternal(self):
        # 从redis中读取最新的status
        hasChange = self.status.sync()

        _logger.debug('_syncInternal status: %s', self.status)
        _logger.debug('_syncInternal status has changed or not: %s', hasChange)

        if hasChange:
            self._reset()

        if not self.status.config:
            time.sleep(3)
            return

        # 因为status发生了变化，需要重新实例化kafkaConsumer和handler
        if self.kafkaConsumer is None:
            self.statusConfig = RedisStatusConfig(self.status.config, forceSync=True)
            self.kafkaConsumer = remote.getKafkaConsumer(
                    self.statusConfig.kafkaGroupId,
                    autoCommit=False
                    )
            self.kafkaConsumer.subscribe([self.relayTopic])
            self.handler = CommonHandler(self.statusConfig)

        while True:
            message = self.kafkaConsumer.poll(1)
            if not self._handleMessage(message):
                break

        self.commit()

    def _handleMessage(self, message):
        msgStatus = self.checkMessage(message)

        if msgStatus == MESSAGE_OK:
            _logger.debug('_handleMessage message: offset[%s]', message.offset())

            event = json.loads(message.value())
            self.handler.syncFromBinlog(event)

            self.prepareCommit(message)

            self.itemCount += 1
            if self.itemCount >= self._PATCH_MESSAGE_COUNT:
                self.itemCount = 0
                return False
        elif msgStatus == MESSAGE_TIMEOUT or msgStatus == MESSAGE_EOF:
            self.itemCount = 0
            return False

        return True

