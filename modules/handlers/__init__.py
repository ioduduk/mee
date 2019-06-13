# -*- coding: utf-8 -*-

from __future__ import print_function, division

import re

"""
Common action
"""
COMMON = 0

INSERT = 0x1 
UPDATE = 0x2
DELETE = 0x4

"""
trigger when INSERT or UPDATE, but not DELETE
"""
DEFAULT_TRIGGER = INSERT | UPDATE
ALL_TRIGGER = INSERT | UPDATE | DELETE

_NOT_TRIGGER_RE = re.compile(r'^\s*~\s*(INSERT|UPDATE|DELETE)\s*$', re.I)
_OR_TRIGGER_RE = re.compile(r'^\s*(INSERT|UPDATE|DELETE)(\s*\|\s*(INSERT|UPDATE|DELETE))*\s*$', re.I)
_ALL_TRIGGER_RE = re.compile(r'^\s*ALL\s*$', re.I)
_KEYWORD_RE = re.compile(r'(INSERT|UPDATE|DELETE)', re.I)

_KEYWORDS = {
        'INSERT': INSERT,
        'UPDATE': UPDATE,
        'DELETE': DELETE
        }

def _replaceKeyword(matchobj):
    keyword = matchobj.group(0).upper()
    return str(_KEYWORDS[keyword])

def parseTrigger(trigger):
    allTrigger = _ALL_TRIGGER_RE.match(trigger)
    if allTrigger:
        return ALL_TRIGGER

    notTirgger = _NOT_TRIGGER_RE.match(trigger)
    if notTirgger:
        exp = _KEYWORD_RE.sub(_replaceKeyword, trigger)
        return eval(exp + ' & ' + str(ALL_TRIGGER))

    orTrigger = _OR_TRIGGER_RE.match(trigger)
    if orTrigger:
        exp = _KEYWORD_RE.sub(_replaceKeyword, trigger)
        return eval(exp)

    return 0

