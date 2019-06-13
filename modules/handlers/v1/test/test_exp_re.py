# -*- coding: utf-8 -*-

from __future__ import print_function, division

import ast
import unittest
from dateutil.parser import parse

from ..commonhandler import _EXP_RE, _SQL_STATEMENT_LIMIT_RE, _PARENT_EXP_RE, _ORIGIN_VALUE_RE

class HandlerRegTests(unittest.TestCase):
    def test_exp_re(self):
        s = '%base.id:(123)%base.name:(\'FF(0)\\\') KK\')%%as_%updated_at'
        matches = _EXP_RE.findall(s)

        doublePer, tableAs, fieldName, defaultValue = matches[0]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, 'base')
        self.assertEqual(fieldName, 'id')
        self.assertEqual(defaultValue, '123')

        doublePer, tableAs, fieldName, defaultValue = matches[1]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, 'base')
        self.assertEqual(fieldName, 'name')
        self.assertEqual(defaultValue, r"'FF(0)\') KK'")

        doublePer, tableAs, fieldName, defaultValue = matches[2]
        self.assertEqual(doublePer, '%%')
        self.assertEqual(tableAs, '')
        self.assertEqual(fieldName, '')
        self.assertEqual(defaultValue, '')

        doublePer, tableAs, fieldName, defaultValue = matches[3]
        self.assertEqual(doublePer, '')
        self.assertEqual(tableAs, '')
        self.assertEqual(fieldName, 'updated_at')
        self.assertEqual(defaultValue, '')

        context = {
                'base': { 'id': '1234', 'name': 'Hello world' },
                '__current': { 'updated_at': '2018-01-01' }
                }

        def sub_exp(matchobj):
            if matchobj.group(0) == '%%':
                return '%'
            tableAs = matchobj.group(2) if matchobj.group(2) else '__current'
            ret = context[tableAs].get(matchobj.group(3), matchobj.group(4))
            return ret

        ss = _EXP_RE.sub(sub_exp, s)
        #s = '%base.id:(123)%base.name:#FF KK#%%as_%updated_at'
        self.assertEqual(ss, '1234Hello world%as_2018-01-01')

    def test_limit_reg(self):
        s = 'select * from user limit  2,100'
        result = _SQL_STATEMENT_LIMIT_RE.search(s)
        self.assertIsNotNone(result)

    def test_parent_reg(self):
        s = 'select * from user where id > %__last.id:(0) and user_id = %__parent.id:(0) and tid=%__parent.kid and %__parent.sid=xid'

        matches = _PARENT_EXP_RE.findall(s)

        self.assertTupleEqual(matches[0], ('user_id', 'id', '', ''))
        self.assertTupleEqual(matches[1], ('tid', 'kid', '', ''))
        self.assertTupleEqual(matches[2], ('', '', 'sid', 'xid'))

    def test_origin_value_reg(self):
        s = ' %id'
        matches = _ORIGIN_VALUE_RE.match(s)
        self.assertIsNone(matches)
        s = '%id '
        matches = _ORIGIN_VALUE_RE.match(s)
        self.assertIsNone(matches)

        s = '%id:(0)'
        matches = _ORIGIN_VALUE_RE.match(s)
        self.assertEqual(matches.group(1), None)
        self.assertEqual(matches.group(2), 'id')
        self.assertEqual(matches.group(3), '0')

        s = '%s.id:(\'abc\')'
        matches = _ORIGIN_VALUE_RE.match(s)
        self.assertEqual(matches.group(1), 's')
        self.assertEqual(matches.group(2), 'id')
        self.assertEqual(matches.group(3), "'abc'")

