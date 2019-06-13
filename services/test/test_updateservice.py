# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..updateservice import *

class UpdateServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.svc = UpdateService(handlerConfigPath='./conf/handlers/config.yml')

    def test_loadConfig(self):
        statusConfig = self.svc._loadConfig()

        self.assertTrue(statusConfig.key)
        self.assertTrue(statusConfig.kafkaGroupId)
        self.assertTrue(statusConfig.esIndexSuffix)
        self.assertTrue(statusConfig.handlerConfig)
        self.assertTrue(statusConfig.synced)
        self.assertNotEqual(statusConfig.key, statusConfig.kafkaGroupId)

    def test_update(self):
        self.svc.update()

