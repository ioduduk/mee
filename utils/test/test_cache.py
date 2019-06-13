# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
from ..cache import cache

class CacheObject(object):
    __metaclass__ = cache
    def __init__(self, key):
        pass

class CacheTests(unittest.TestCase):
    def test_cache(self):
        one = CacheObject('123')
        two = CacheObject('123')
        self.assertIs(one, two)

