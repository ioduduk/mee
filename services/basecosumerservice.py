# -*- coding: utf-8 -*-

from __future__ import print_function, division

import time

import modules.remote as remote
import application.app as app

from confluent_kafka import TopicPartition, KafkaError
from application.config import config

_logger = app.getLogger('base')

MESSAGE_OK = 1
MESSAGE_TIMEOUT = 2
MESSAGE_EOF = 3
MESSAGE_ERROR = 4

class BaseConsumerService(object):
    def __init__(self):
        self.commitCache = {}

    def checkMessage(self, message):
        if message is None:
            return MESSAGE_TIMEOUT
        else:
            error = message.error()
            if not error:
                return MESSAGE_OK
            elif error.code() == KafkaError._PARTITION_EOF:
                return MESSAGE_EOF

            return MESSAGE_ERROR

    def prepareCommit(self, message):
        topic = message.topic()
        partition = message.partition()
        offset = message.offset()
        key = message.key()

        cacheKey = topic + '_' + str(partition)

        if cacheKey in self.commitCache:
            self.commitCache[cacheKey].offset = offset
        else:
            self.commitCache[cacheKey] = TopicPartition(topic, partition, offset)

    def commit(self):
        if not self.commitCache:
            self.kafkaConsumer.commit()
        else:
            self.kafkaConsumer.commit(offsets=self.commitCache.values())
            self.commitCache.clear()
