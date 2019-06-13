# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import

class IllegalArgumentException(RuntimeError):
    """
    illegal argument exception.
    """

class LogicException(RuntimeError):
    """
    biz logic exception.
    """
    def __init__(self, errmsg, errno=-1):
        errinfo = {
                'errno': errno,
                'errmsg': errmsg
                }
        super(LogicException, self).__init__(str(errinfo))

class IllegalConfigException(RuntimeError):
    """
    illegal config.
    """

class NotSupportedException(RuntimeError):
    """
    Not supported.
    """
