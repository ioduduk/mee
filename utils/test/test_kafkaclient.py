# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..kafkaclient import KafkaConsumer, KafkaProducer
from application.config import config

import application.app as app

class KafkaClientTests(unittest.TestCase):
    """
    """
    @classmethod
    def setUpClass(self):
        self.host = config().get('kafka', 'host')
        self.groupId = '__group_id_' + app.getUuid()

    def test_kafka_producer(self):
        """
        """
        producer = KafkaProducer(self.host)
        self.assertTrue(callable(producer.list_topics))

    def test_kafka_consumer(self):
        """
        """
        consumer = KafkaConsumer(self.groupId, self.host)
        self.assertTrue(callable(consumer.list_topics))

    @classmethod
    def tearDownClass(self):
        """
        """

