# -*- coding: utf-8 -*-

"""
可以在配置文件中以 %{xxx()} 形式执行的函数集合。
注意不要增加与安全相关的函数。
"""

from __future__ import print_function, division

import os
import sys
import random
from datetime import datetime

def getpid():
    return os.getpid()

def getuid():
    return os.getuid()

def getdate(fmt='%Y-%m-%d'):
    now = datetime.now()
    return now.strftime(fmt)

def randint(min=0, max=sys.maxsize):
    return random.randint(min, max)

