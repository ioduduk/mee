# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest

import zope.interface
from zope.interface.verify import verifyObject

from ..interfaces import *
from ..status import *
from ..handlers.v1 import *

class InterfaceTests(unittest.TestCase):
    """
    """
    def test_interface(self):
        self.verifyInterface(IStatus, RedisStatus)
        self.verifyInterface(IStatusConfig, RedisStatusConfig, None)
        self.verifyInterface(IStatusConfig, RedisStatusConfig, None)
        self.verifyInterface(IHandlerConfig, HandlerConfig)
        self.verifyInterface(IHandler, CommonHandler, RedisStatusConfig(''))

    def verifyInterface(self, interfaceClass, objectClass, *args, **kwargs):
        obj = objectClass(*args, **kwargs)

        self.assertTrue(interfaceClass.implementedBy(objectClass))
        self.assertTrue(interfaceClass.providedBy(obj))
        self.assertTrue(verifyObject(interfaceClass, obj))




