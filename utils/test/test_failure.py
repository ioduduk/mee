# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
from ..failure import Failure

class FailureTests(unittest.TestCase):
    def test_failure(self):
        try:
            raise Exception('test failure 1234567890')
        except Exception as e:
            failure = Failure()
            strFailure = str(failure)
            self.assertTrue(strFailure.find('test_failure.py') >= 0)
            self.assertTrue(strFailure.find('test failure 1234567890') >= 0)
