# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
import datetime

import utils

from modules.handlers import *

from application import IllegalConfigException

class TriggerTests(unittest.TestCase):
    def test_trigger(self):
        trigger = '~DELETE'
        result = parseTrigger(trigger)
        self.assertEqual(result, INSERT|UPDATE)

        trigger = 'DELETE'
        result = parseTrigger(trigger)
        self.assertEqual(result, DELETE)

        trigger = 'INSERT|DELETE'
        result = parseTrigger(trigger)
        self.assertEqual(result, INSERT|DELETE)

        trigger = 'INSERT | DELETE'
        result = parseTrigger(trigger)
        self.assertEqual(result, INSERT|DELETE)

        trigger = 'INSERT|UPDATE|DELETE'
        result = parseTrigger(trigger)
        self.assertEqual(result, INSERT|UPDATE|DELETE)

        trigger = 'ALL'
        result = parseTrigger(trigger)
        self.assertEqual(result, INSERT|UPDATE|DELETE)

        trigger = '~ DELETE | INSERT '
        result = parseTrigger(trigger)
        self.assertEqual(result, 0)
