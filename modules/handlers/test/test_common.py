# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
import datetime

import utils

import modules.handlers.common as common
from application import IllegalConfigException

class CommonTests(unittest.TestCase):
    def test_sum(self):
        pass

    def test_yesterday(self):
        pass

    def test_resolve_args(self):
        argsStr = 'a'
        result = common._resolveArgs(argsStr)
        self.assertListEqual(result, ['a'])

        argsStr = 'a, bc'
        result = common._resolveArgs(argsStr)
        self.assertListEqual(result, ['a', 'bc'])

        argsStr = 'a(), bc'
        result = common._resolveArgs(argsStr)
        self.assertListEqual(result, ['a()', 'bc'])

        argsStr = 'a(a1, a2, a3), bc'
        result = common._resolveArgs(argsStr)
        self.assertListEqual(result, ['a(a1, a2, a3)', 'bc'])

        argsStr = 'a(a1, a2, a3), bc, d(f1, f(k1,k2))'
        result = common._resolveArgs(argsStr)
        self.assertListEqual(result, ['a(a1, a2, a3)', 'bc', 'd(f1, f(k1,k2))'])

        try:
            argsStr = 'a(a1, a2, a3), bc, d(f1, f(k1,k2)'
            result = common._resolveArgs(argsStr)
        except Exception as ex:
            self.assertIsInstance(ex, IllegalConfigException)

    def test_resolve(self):
        values = {
                'f1': '2018-12-12 08:23:12',
                'f2': 2,
                'f3': 3,
                'f4': 4,
                'f5': 2.1,
                'f6': -3.1,
                'f7': 'abcdef',
                'f8': ''
                }

        funcStr = "echo(1)"
        result = common.resolve(funcStr, values=values, action='DELETE')
        self.assertEqual(result, 1)

        funcStr = "echo('1')"
        result = common.resolve(funcStr, values=values, action='DELETE')
        self.assertEqual(result, '1')

        funcStr = "yesterday(f1)"
        result = common.resolve(funcStr, values=values, action='UPDATE')
        self.assertEqual(result, datetime.datetime(2018, 12, 11, 8, 23, 12)) 

        funcStr = "max(f2, f3)"
        result = common.resolve(funcStr, values=values, action='COMMON')
        self.assertEqual(result, 3)

        funcStr = "min(f2, f3)"
        result = common.resolve(funcStr, values=values, action='')
        self.assertEqual(result, 2)

        funcStr = "sum(f2, f3, f6)"
        result = common.resolve(funcStr, values=values, action='')
        self.assertEqual(result, 1.9)

        funcStr = "sum(max(f3, f4), -f2, echo(1))"
        result = common.resolve(funcStr, values=values, action='')
        self.assertEqual(result, 3) 




