# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
from dateutil.parser import parse

from ..commonhandler import *
from ..commonhandler import _EXP_RE, _SQL_STATEMENT_LIMIT_RE
from ...handlerconfig import *
from ....status import *

class CommonHandlerUtilTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        handlerConfig = HandlerConfig()
        handlerConfig.loadFromFile('./conf/handlers/config.yml')

        statusConfig = RedisStatusConfig('1234567890')
        statusConfig.esIndexSuffix = '123456'
        statusConfig.handlerConfig = handlerConfig

        self.handler = CommonHandler(statusConfig)

    def test_exp_re(self):
        s = '%base.id:(123)%base.name:#FF KK#%%as_%updated_at'
        matches = _EXP_RE.findall(s)

        doublePer, tableAs, fieldName, _, defaultValue = matches[0]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, 'base')
        self.assertEqual(fieldName, 'id')
        self.assertEqual(_, '')
        self.assertEqual(defaultValue, '123')

        doublePer, tableAs, fieldName, _, defaultValue = matches[1]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, 'base')
        self.assertEqual(fieldName, 'name')
        self.assertEqual(_, '#')
        self.assertEqual(defaultValue, 'FF KK')

        doublePer, tableAs, fieldName, _, defaultValue = matches[2]
        self.assertEqual(doublePer, '%%')
        self.assertEqual(tableAs, '')
        self.assertEqual(fieldName, '')
        self.assertEqual(_, '')
        self.assertEqual(defaultValue, '')

        doublePer, tableAs, fieldName, _, defaultValue = matches[3]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, '')
        self.assertEqual(fieldName, 'updated_at')
        self.assertEqual(_, '')
        self.assertEqual(defaultValue, '')

        context = {
                'base': { 'id': '1234', 'name': 'Hello world' },
                '__current': { 'updated_at': '2018-01-01' }
                }

        def sub_exp(matchobj):
            if matchobj.group(0) == '%%':
                return '%'
            tableAs = matchobj.group(2) if matchobj.group(2) else '__current'
            ret = context[tableAs].get(matchobj.group(3), matchobj.group(5))
            return ret

        ss = _EXP_RE.sub(sub_exp, s)
        #s = '%base.id:(123)%base.name:#FF KK#%%as_%updated_at'
        self.assertEqual(ss, '1234Hello world%as_2018-01-01')

    def test_limit_reg(self):
        s = 'select * from user limit  2,100'
        result = _SQL_STATEMENT_LIMIT_RE.search(s)
        self.assertIsNotNone(result)

    def test_filter(self):
        filterDict = {
                'type': 1,
                'status': { '!=': 0 },
                'choise': [1, 2, 4, 6],
                'time' : { '<=': 3 }
                }

        value = {
                'time': 3,
                'a': '123qwe',
                'type': 1,
                'status': 1,
                'choise': 2
                }
        result = self.handler._filterData(filterDict, value)
        self.assertTrue(result)

        value = {
                'time': 1,
                'a': '123qwe',
                'type': 1,
                'status': 0,
                'choise': 2
                }
        result = self.handler._filterData(filterDict, value)
        self.assertFalse(result)
