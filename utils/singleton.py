# -*- coding: utf-8 -*-

"""
A metaclass for singleton pattern.
"""

import threading
import weakref

class singleton(type):
    def __init__(self, *args, **kwargs):
        self._instance = None
        self._rlock = threading.RLock()
        super(singleton, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        if self._instance is None:
            with self._rlock:
                if self._instance is None:
                    self._instance = super(singleton, self).__call__(*args, **kwargs)

        return self._instance


class cache(type):
    def __init__(self, *args, **kwargs):
        self._cache = weakref.WeakValueDictionary()
        self._rlock = threading.RLock()
        super(cache, self).__init__(*args, **kwargs)

    def __call__(self, key, *args, **kwargs):
        if key not in self._cache:
            with self._rlock:
                if key not in self._cache:
                    obj = super(cache, self).__call__(key, *args, **kwargs)
                    self._cache[key] = obj
                    return obj

        return self._cache[key]


