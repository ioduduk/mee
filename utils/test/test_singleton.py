# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
from ..singleton import singleton

class SingleObject():
    __metaclass__ = singleton
    def __init__(self, value):
        self.value = value

class SingletonTests(unittest.TestCase):
    """
    """
    def test_singleton(self):
        one = SingleObject(1)
        two = SingleObject(2)

        self.assertEqual(one.value, 1)
        self.assertEqual(one.value, two.value)

        self.assertIs(one, two)

        one.a = [1, 2]
        self.assertSequenceEqual([1, 2], two.a)

        three = SingleObject(3)
        self.assertSequenceEqual([1, 2], three.a)
