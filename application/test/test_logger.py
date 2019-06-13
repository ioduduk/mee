# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

import os
import logging
import application.app as app
from ..app import init as initApp

class LoggerTests(unittest.TestCase):
    def test_logging(self):
        logger = app.getLogger()
        logger.info('app logger hello word!!!')





