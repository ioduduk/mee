# -*- coding: utf-8 -*-

from __future__ import print_function, division

import logging

from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions, ConfigResource
from confluent_kafka import KafkaException, KafkaError, libversion, Consumer, Producer
import confluent_kafka
import concurrent.futures

class KafkaAdminClient(object):
    def __init__(self, brokers):
        self.adminclient = AdminClient({
                'bootstrap.servers': brokers
                })
    
    def __getattr__(self, name):
        method = getattr(self.adminclient, name)
        return method

class KafkaProducer(object):
    def __init__(self, brokers):
        self.producer = Producer({
                'bootstrap.servers': brokers
                })
    
    def __getattr__(self, name):
        method = getattr(self.producer, name)
        return method

    def __del__(self):
        if self.producer:
            self.producer.flush()


class KafkaConsumer(object):
    def __init__(self, groupId, brokers, autoCommit=True, autoOffsetReset='earliest'):
        self.consumer = Consumer({
                'bootstrap.servers': brokers,
                'group.id': groupId,
                'enable.auto.commit': 'true' if autoCommit else 'false',
                'default.topic.config': {
                    'auto.offset.reset': autoOffsetReset
                }
                })
        self.closed = False
    
    def __getattr__(self, name):
        method = getattr(self.consumer, name)
        return method

    def close(self):
        if not self.closed and self.consumer:
            self.consumer.close()
            self.closed = True

    def __del__(self):
        self.close()

