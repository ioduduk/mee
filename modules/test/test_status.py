# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
import time
import random
import yaml

import zope.interface
from zope.interface.verify import verifyObject

from ..interfaces import *
from ..status import *

import application.app as app
import modules.remote as remote

class StatusTests(unittest.TestCase):
    """
    """
    @classmethod
    def setUpClass(self):
        status = RedisStatus()
        status.sync()

        self.oldStatus = status

    @classmethod
    def tearDownClass(self):
        status = RedisStatus()
        status.update()

    def test_a_create(self):
        status = RedisStatus()
        status.code = 'test'
        status.create()

    def test_sync(self):
        status = RedisStatus()
        status.sync()

    def test_fromDict(self):
        status = RedisStatus.fromDict({'code': 'test'})

    def test_equal(self):
        status = RedisStatus(code='1')
        status1 = RedisStatus(code='1')
        status2 = RedisStatus(code='2')

        self.assertTrue(status == status1)
        self.assertEqual(status, status1)
        self.assertFalse(status == status2)
        self.assertTrue(status != status2)

    def test_str(self):
        status = RedisStatus(code='1')


class StatusConfigTest(unittest.TestCase):
    """
    """
    @classmethod
    def setUpClass(self):
        self.configKey = 'test_' + str(int(time.time())) + '_' + str(random.randint(1, 1000000))

    def test_0_set(self):
        statusConfig = RedisStatusConfig(self.configKey)
        statusConfig.kafkaGroupId = 'gid_1'
        statusConfig.esIndexSuffix = 'es_index_1'
        statusConfig.handlerConfig = HandlerConfig()
        statusConfig.handlerConfig.loadFromFile('./conf/handlers/config.yml')
        self.assertTrue(statusConfig.set())

    def test_a_sync(self):
        statusConfig = RedisStatusConfig(self.configKey)
        statusConfig.syncByKey()
        self.assertTrue(statusConfig.synced)
        self.assertEqual(statusConfig.kafkaGroupId, 'gid_1')
        self.assertEqual(statusConfig.esIndexSuffix, 'es_index_1')

    def test_z_delete(self):
        statusConfig = RedisStatusConfig(self.configKey)
        r1 = statusConfig.delete()
        r2 = statusConfig.delete()
        self.assertEqual(r1, 1)
        self.assertEqual(r2, 0)
        self.assertFalse(statusConfig.synced)

    @classmethod
    def tearDownClass(self):
        redisclient = remote.getRedisClient()
        redisclient.expire(self.configKey, 3)    








