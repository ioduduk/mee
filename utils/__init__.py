# -*- coding: utf-8 -*-

"""
工具类。
"""

import inspect

def _attrForName(name):
    parts = name.split('.')
    moduleName = '.'.join(parts[:-1])
    className = parts[-1]
    module = __import__(moduleName, globals(), locals(), [className], -1)
    return getattr(module, className, None)
    

def classForName(name):
    clazz = _attrForName(name)
    if inspect.isclass(clazz):
        return clazz
    else:
        return None

def functionForName(name):
    func = _attrForName(name)
    if inspect.isfunction(func):
        return func
    else:
        return None


