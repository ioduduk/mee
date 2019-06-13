# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..commonhandler import HanlderContext

class HanlderContextUtilTests(unittest.TestCase):
    def test_context(self):
        data = {
                'a': 1,
                'b': 2,
                'c': 'qwe'
                }

        context = HanlderContext(1, **data)
        keys = context.keys()
        self.assertListEqual(keys, data.keys())

        values = context.values()
        self.assertListEqual(values, data.values())

        keys = []
        for key in context:
            keys.append(key)
        self.assertListEqual(keys, data.keys())

        keys = []
        values = []
        for item in context.items():
            key, value = item
            keys.append(key)
            values.append(value)
        self.assertListEqual(keys, data.keys())
        self.assertListEqual(values, data.values())

        context['d'] = 4
        self.assertEqual(context['d'], 4)

        del context['d']
        self.assertFalse('d' in context)

        self.assertEqual(len(context), 3)

        a = context.get('a')
        self.assertEqual(a, 1)
        a = context.get('a', None)
        self.assertEqual(a, 1)

        n = context.get('n', 12)
        self.assertEqual(n, 12)

        n = context.get('n', None)
        self.assertIsNone(n)

        n = context.get('n')
        self.assertIsNone(n)





