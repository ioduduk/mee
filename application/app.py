# -*- coding: utf-8 -*-

from __future__ import print_function, division, absolute_import

import os
import sys
import logging
import hashlib
import time
import socket
import random

from utils.singleton import singleton
from .config import config
from .vars import chained_vars, notfound
from ._logger import _initLogging as initLogging

class environ(object):
    __metaclass__ = singleton

    def __init__(self):
        self.vars = chained_vars(os.environ, notfound())
        self.base = self.vars

def getLogger(name=None):
    loggerName = config().getAppName()
    if name:
        loggerName = (loggerName + '.' + name) if loggerName else name

    return logging.getLogger(loggerName)

def init(conf=''):
    """
    初始化。
    conf: 配置文件的路径
    """
    config().load(conf)
    initLogging()
    
def getUuid(action=''):
    m = hashlib.md5()

    uuidSrc = [
            action,
            socket.gethostname(),
            str(os.getpid()),
            str(time.time()),
            str(random.randint(1, sys.maxsize))
            ]

    m.update('_'.join(uuidSrc))

    return m.hexdigest()

def getPrjRoot():
    prjRoot = os.path.realpath(os.path.abspath(os.path.dirname(__file__) + "/../"))
    return prjRoot
