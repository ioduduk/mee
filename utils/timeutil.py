# -*- coding: utf-8 -*-

"""
实现一些简便的时间戳转换的方法
"""

import time
import re
from datetime import datetime
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

_NOW_RE = re.compile(r'^\s*now\s*$', re.I)
_DELTA_RE = re.compile(r'^\s*((?:[+-]?)\d+)\s*(second|minute|hour|day|week|month|year)(?:s?)\s*$', re.I)

def strtotime(timestr):
    """
    根据表示日期的字符串，返回时间戳
    """
    dtime = parse(timestr)
    return int(time.mktime(dtime.timetuple()))

def deltatotime(deltastr, anchor=None):
    """
    根据形如 +1 day, -1 month的字符串，与当前时间比较，得出一个datetime类型的结果
    """
    anchorTime = datetime.now() if anchor is None else (parse(anchor) if isinstance(anchor, basestring) else anchor)

    if _NOW_RE.match(deltastr):
        return anchorTime

    matches = _DELTA_RE.match(deltastr)
    if matches:
        value = int(matches.group(1))
        deltaType = matches.group(2)
        if deltaType == 'year':
            delta = relativedelta(years=value)
        elif deltaType == 'month':
            delta = relativedelta(months=value)
        elif deltaType == 'week':
            delta = relativedelta(weeks=value)
        elif deltaType == 'day':
            delta = relativedelta(days=value)
        elif deltaType == 'hour':
            delta = relativedelta(hours=value)
        elif deltaType == 'minutes':
            delta = relativedelta(minutes=value)
        elif deltaType == 'seconds':
            delta = relativedelta(seconds=value)

        return anchorTime + delta
    
    return None

def rangePeriod(startDate, deltaStr):
    '''
    返回一个日期list，包含了 >= startDate 并 <= (startDate + deltaStr) 的所有日期
    '''
    dates = []
    startDatetime = parse(startDate) if isinstance(startDate, basestring) else startDate
    deltaDatetime = deltatotime(deltaStr, startDatetime)
    currDatetime, endDatetime = (startDatetime, deltaDatetime) if startDatetime < deltaDatetime else (deltaDatetime, startDatetime)
    while currDatetime <= endDatetime:    
        dates.append(currDatetime.strftime('%Y-%m-%d'))
        currDatetime += relativedelta(days=+1)

    return dates

def dateHandler(obj):
    if hasattr(obj, 'isoformat'):
        if isinstance(obj, datetime):
            return obj.isoformat(" ").split(".")[0]
        return obj.isoformat()
