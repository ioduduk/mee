# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..app import environ, getUuid

class AppTests(unittest.TestCase):
    """
    """
    def test_environ_singleton(self):
        one = environ()
        two = environ()

        self.assertIs(one, two)

        one.a = [1, 2]
        self.assertSequenceEqual([1, 2], two.a)

        three = environ()
        self.assertSequenceEqual([1, 2], three.a)

    def test_getUuid(self):
        uuid1 = getUuid()
        uuid2 = getUuid()
        self.assertNotEqual(uuid1, uuid2)
        
