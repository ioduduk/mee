# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import traceback

class Failure(object):
    def __init__(self, excType=None, excValue=None, excTrackback=None):
        if excType is None:
            self.excType, self.excValue, self.excTrackback = sys.exc_info()
        else:
            self.excType, self.excValue, self.excTrackback = (excType, excValue, excTrackback)

    def __str__(self):
        return str(traceback.format_exception(self.excType, self.excValue, self.excTrackback))

    def __del__(self):
        del self.excTrackback
            
