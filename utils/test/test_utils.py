# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import traceback

import unittest
import yaml

import utils
from modules.status import *
from ..failure import Failure

class UtilsTests(unittest.TestCase):
    def test_calssforname(self):
        clazz = utils.classForName('modules.handlers.v1.CommonHandler')
        import modules.handlers.v1 as v1
        self.assertIs(clazz, v1.CommonHandler)

        handlerConfig = HandlerConfig()
        handlerConfig.loadFromFile('./conf/handlers/config.yml')

        statusConfig = RedisStatusConfig('1234567890')
        statusConfig.esIndexSuffix = '123456'
        statusConfig.handlerConfig = handlerConfig

        handler = clazz(statusConfig)

        self.assertIsInstance(handler, v1.CommonHandler)

    def _test_failure(self):
        try:
            raise Exception('test traceback')
        except Exception as e:
            failure = Failure()
            print(failure)
