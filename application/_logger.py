# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import os
import logging
import logging.config

from datetime import datetime
from .config import config

try:
    import codecs
except ImportError:
    codecs = None

_DEFAULT_LOGGING_FORMAT = r'[%(asctime)s] %(name)s.%(levelname)s pid:%(process)d file:%(pathname)s line:%(lineno)s func:%(funcName)s message:[%(message)s]'
_DEFAULT_LOGGING_DATEFMT = r'%Y-%m-%d %H:%M:%S'
_DEFAULT_LOGGING_LEVEL = r'DEBUG'
_DEFAULT_LOGGING_FILEHANDLER = r'application._logger._DatetimedFileHandler'

class _DatetimedFileHandler(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None):
        if codecs is None:
            encoding = None

        self.basename = filename
        self.currFilename = self._getFilename(filename)

        logging.FileHandler.__init__(self, self.currFilename, mode, encoding, 1)

    def _getFilename(self, filename):
        return datetime.now().strftime(filename)

    def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        self.baseFilename = os.path.abspath(self.currFilename)
        return logging.FileHandler._open(self)

    def shouldReopen(self, record):
        currFilename = self._getFilename(self.basename)
        if currFilename != self.currFilename:
            self.currFilename = currFilename
            return True
        return False

    def reopen(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        self.stream = self._open()

    def emit(self, record):
        try:
            if self.shouldReopen(record):
                self.reopen()
            logging.FileHandler.emit(self, record)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

class _SimpleLoggerFilter(logging.Filter):
    def __init__(self, foo=None):
        pass

    def filter(self, record):
        return True

def _getloggerConfig(name, fallback=None):
    return config().get('logger', name, fallback)

def _initLogging():
    """
    初始化日志组件
    """
    loggerLevel = _getloggerConfig('level', 'DEBUG')
    loggingDict = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'simpleFormatter': {
                'format': _getloggerConfig('format', _DEFAULT_LOGGING_FORMAT),
                'datefmt': _getloggerConfig('datefmt', _DEFAULT_LOGGING_DATEFMT)
            },
        },
        'filters': {
            'simpleFilter': {
                '()': _SimpleLoggerFilter,
                'foo': 'bar',
            }
        },
        'handlers': {
            'null': {
                'level': 'DEBUG',
                'class': 'logging.NullHandler'
            },
            'console': {
                'level': 'DEBUG',
                'class': 'logging.StreamHandler',
                'stream': sys.stderr,
                'formatter': 'simpleFormatter'
            },
            'filehandler': {
                'level': _getloggerConfig('level', _DEFAULT_LOGGING_LEVEL),
                'class': _DEFAULT_LOGGING_FILEHANDLER,
                'filename': _getloggerConfig('filename'),
                'mode': 'a',
                'encoding': 'utf-8',
                'formatter': 'simpleFormatter'
            }
        },
        'loggers': {
            '': {
                'handlers': ['null'],
                'level': loggerLevel,
            },
            config().getAppName(): {
                'handlers': ['filehandler'],
                'level': loggerLevel,
                'propagate': False,
                'filters': ['simpleFilter']
            }
        }
    }
    
    logging.config.dictConfig(loggingDict)

