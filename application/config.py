# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import

import re

from utils.singleton import singleton
from configparser import ConfigParser, ExtendedInterpolation
import application.inner as inner

_FUNCTION_RE = re.compile(r'(%%)|(%{(\w+\(.*?\))})')
_DEFAULT_APP_NAME = 'application'

class config(object):
    __metaclass__ = singleton

    def __init__(self, args=None):
        self.parser = ConfigParser(
                defaults=args,
                default_section='global',
                interpolation=ExtendedInterpolation()
                )

    def load(self, filename):
        self.parser.read(filename, encoding='utf-8')

    def get(self, section, option, fallback=None):
        value = self.parser.get(section, option, fallback=fallback)
        if value:
            value = _FUNCTION_RE.sub(self.sub_func, value)

        return value

    def getAppName(self):
        return self.get('global', 'app_name', _DEFAULT_APP_NAME)

    def sub_func(self, match):
        if match.group(0) == '%%':
            return '%'
        
        fun = 'inner.' + str(match.group(3))
        res = unicode(eval(fun))
        return res

    def sections(self, pattern=None):
        sections = self.parser.sections()
        result = []

        if pattern:
            for sec in sections:
                if re.match(pattern, sec):
                    result.append(sec)
        else:
            result = sections[:]

        return result



