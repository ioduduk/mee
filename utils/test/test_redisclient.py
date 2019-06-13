# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..redisclient import RedisClient
from application.config import config

import application.app as app

class RedisClientTests(unittest.TestCase):
    """
    """
    @classmethod
    def setUpClass(self):
        host = config().get('redis', 'host')
        port = config().get('redis', 'port')
        db = config().get('redis', 'db')
        self.client = RedisClient(host, port, db)

        self.key = '__redis_key_' + app.getUuid()

    def test_redis_proxy(self):
        self.client.set(self.key, 'bar')
        bar = self.client.get(self.key)
        self.assertEqual(bar, 'bar')

        count = self.client.delete(self.key)
        self.assertEqual(count, 1)

    def test_distribute_lock(self):
        lockKey = 'test_' + app.getUuid()

        lock = self.client.acquireDLock(lockKey)

        lockAgain = self.client.acquireDLock(lockKey)
        self.assertFalse(lockAgain)

        releaseResult = self.client.releaseDLock(lock)
        self.assertTrue(releaseResult)

