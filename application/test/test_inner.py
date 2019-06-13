# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

import os
from ..inner import *

class InnerTests(unittest.TestCase):
    def test_getpid(self):
        pid1 = getpid()
        pid2 = os.getpid()
        self.assertEqual(pid1, pid2)

    def test_randint(self):
        rand1 = randint()
        self.assertTrue(rand1 >= 0)

        rand2 = randint(1, 1)
        self.assertTrue(rand2 == 1)

        rand3 = randint(min=1, max=10)
        self.assertTrue(rand3 >= 1 and rand3 <= 10)





