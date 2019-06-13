# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

from ..timeutil import *

class TimeutilTests(unittest.TestCase):
    def test_strtotime(self):
        time = strtotime('2018-07-02 08:13:14')
        self.assertEqual(1530490394, time)

    def test_deltatotime(self):
        dtime = deltatotime('-1day', '2018-07-02 08:13:14')
        self.assertEqual(dtime.strftime('%Y-%m-%d %H:%M:%S'), '2018-07-01 08:13:14')

    def test_rangePeriod(self):
        range = rangePeriod('2018-07-01 10:11:21', '+1 weeks')
        self.assertSequenceEqual(
                range,
                ['2018-07-01', '2018-07-02', '2018-07-03', '2018-07-04', '2018-07-05', '2018-07-06', '2018-07-07', '2018-07-08']
        )


