# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import

class var_undefined(Exception):
    """
    variable has not defined yet.
    """

class notfound(object):
    def __getattr__(self, name):
        raise var_undefined('undefined variable [%s]' % name)

class chained_vars(object):
    def __init__(self, currVars, prevVars):
        self._currVars = currVars
        self._prevVars = prevVars

