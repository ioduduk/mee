# -*- coding: utf-8 -*-

"""
可以在config.yml配置文件中配置的函数集合。
注意不要增加与安全相关的函数。
"""

from __future__ import print_function, division

import os
import sys
import re
import random
import ast
import __builtin__

from datetime import datetime
from numbers import Number
from functools import wraps

import utils
import utils.timeutil as timeutil
import application.app as app

from application import IllegalConfigException
from application.connection import ConnectinoPool

_FUNCTION_RE = re.compile(r'^\s*((?:\w+\.)*\w+)\((.*)\)\s*$')

_logger = app.getLogger('base')

def resolve(funcString, **kwargs):
    if not funcString:
        return None

    funcInfo = _resolveFunction(funcString)
    if funcInfo is None:
        values = kwargs['values']

        if funcString[0] in ('+', '-'):
            sign = funcString[0]
            funcString = funcString[1:]
            oriValue = values[funcString]
            if isinstance(oriValue, Number):
                if sign == '-':
                    return -oriValue
                else:
                    return oriValue
            else:
                raise IllegalConfigException('field with a sign(+ or -) must be a number type: %s' % funcString)
        else:
            return values[funcString]
    else:
        funcName = funcInfo['name']
        args = funcInfo['args']

        if funcName == 'echo':
            return echo(*args)

        func = getattr(sys.modules[__name__], funcName, None)
        if func is None:
            func = utils.functionForName(funcName)

        if func is None:
            raise IllegalConfigException('can NOT find function with name[%s]' % funcName)

        argv = []
        for arg in args:
            argv.append(resolve(arg, **kwargs))

        return func(*argv, **kwargs)
        
def _resolveFunction(functionStr):
    parts = _FUNCTION_RE.match(functionStr)
    if parts is None:
        return None

    argsStr = parts.group(2)
    args = _resolveArgs(argsStr)

    result = {
            'name': parts.group(1),
            'args': args
            }

    return result

def _resolveArgs(argsStr):
    stack = []
    result = []
    buff = []
    inQuoted = None
    lastChar = None
    for char in argsStr:
        if inQuoted:
            if char == inQuoted and lastChar != '\\':
                inQuoted = None
            buff.append(char)
        elif char not in ('(', ')', ','):
            if char in ('"', '\'') and lastChar != '\\':
                inQuoted = char

            buff.append(char)
        elif char == ',':
            if stack:
                buff.append(char)
            else:
                result.append(''.join(buff))
                buff = []
        elif char == '(':
            stack.append(char)
            buff.append(char)
        elif char == ')':
            stack.pop()
            buff.append(char)

        lastChar = char

    if stack:
        raise IllegalConfigException('args string is NOT valid: %s' % argsStr)

    if buff:
        result.append(''.join(buff))

    result = [ x.strip() for x in result ]

    return result

def echo(argv, **kwargs):
    if argv is None:
        return None

    return ast.literal_eval(argv)

def yesterday(aDatetime, **kwargs):
    return timeutil.deltatotime('-1day', aDatetime)

def max(*argv, **kwargs):
    return __builtin__.max(argv)

def min(*argv, **kwargs):
    return __builtin__.min(argv)

def sum(*argv, **kwargs):
    return __builtin__.sum(argv)

def abs(argv, **kwargs):
    return __builtin__.abs(argv)

def executeSQL(sql, *sqlArgs, **kwargs):
    database = kwargs['database']
    conn = ConnectinoPool().connection(database)

    with conn.cursor() as cursor:
        cursor.execute(sql, sqlArgs)
        data = cursor.fetchall()
        _logger.debug('excuteSQL sql[%s], args[%s], data[%s]', sql, sqlArgs, data)

    if data:
        result = []
        for item in data:
            result.append(item.values()[0])

        if len(result) == 1:
            return result[0]
        
        return result

    return None

