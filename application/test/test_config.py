# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import

import os
import unittest
from datetime import datetime

from ..config import config

class ConfigTests(unittest.TestCase):
    def test_config_get(self):
        conf = config()

        value = conf.get('unittest', 'name')
        self.assertEqual(u'unittest', value)

        value = conf.get('unittest', 'cnname')
        self.assertEqual(u'单元测试', value)

        value = conf.get('unittest', 'filename')
        self.assertEqual(u'logs/unittest.log', value)

        pid = os.getpid()

        value = conf.get('unittest', 'pidname')
        self.assertEqual(u'pid' + unicode(pid) + u'.pid', value)

        value = conf.get('unittest', 'pidname1')
        self.assertEqual(u'pid%{getpid()}.pid', value)

        value = conf.get('unittest', 'pidname2')
        self.assertEqual(u'pid%' + unicode(pid) + u'.pid', value)

        #pidname3=pid%{getpid()}%{getpid()}.pid
        value = conf.get('unittest', 'pidname3')
        self.assertEqual(u'pid' + unicode(pid) + unicode(pid) + u'.pid', value)

        value = conf.get('unittest', 'format')
        self.assertEqual(u'%(asctime)s', value)

        value = conf.get('unittest', 'not_exist')
        self.assertTrue(value is None)

        value = conf.get('unittest', 'not_exist', 'default_value')
        self.assertEqual(value, 'default_value')

        value = conf.get('unittest', 'level')
        self.assertEqual(value, 'DEBUG')

        value  = conf.get('unittest', 'date')
        self.assertEqual(value, 'now_is_%s.date' % (datetime.now().strftime('%Y-%m-%d')))

        value  = conf.get('unittest', 'date1')
        self.assertEqual(value, 'now_is_%s.date' % (datetime.now().strftime('%Y-%m')))



