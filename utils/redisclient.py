# -*- coding: utf-8 -*-

from __future__ import print_function, division

import redis
import logging

from redlock import Redlock, MultipleRedlockException

class RedisClient(object):
    def __init__(self, host, port=6379, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, db=db)
        self.redlock = Redlock([self.redis])
    
    def __getattr__(self, name):
        method = getattr(self.redis, name)
        return method

    def acquireDLock(self, resource, ttl=300):
        """
        获取分布式锁。
        ttl: 锁的过期时间，单位是秒，默认300秒。
        """
        resource = 'lock_' + resource
        try:
            return self.redlock.lock(resource, ttl * 1000)
        except MultipleRedlockException as e:
            logging.error('Error acquiring dlock: %s', e)
            return False

    def releaseDLock(self, lock):
        """
        释放分布式锁
        """
        try:
            self.redlock.unlock(lock)
            return True
        except MultipleRedlockException as e:
            logging.error('Error releasing dlock: %s', e)
            return False

