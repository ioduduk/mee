# -*- coding: utf-8 -*-

from __future__ import print_function, division

import threading
import pymysql.cursors

from utils.singleton import singleton
from .config import config

class ConnectinoPool(object):
    __metaclass__ = singleton

    def __init__(self):
        self.config = config()
        self._rlock = threading.RLock()
        self.connections = {}

    def connection(self, database):
        if not database:
            return None

        if database not in self.connections:
            with self._rlock:
                if database not in self.connections:
                    self.connections[database] = self._connect(database)

        conn = self.connections[database]
        if not conn.open:
            conn.ping(reconnect=True)

        return conn

    def _connect(self, database):
        section = 'mysql:' + str(database)
        conn = pymysql.connect(
                host=self.config.get(section, 'host'),
                port=int(self.config.get(section, 'port')),
                db=self.config.get(section, 'database'),
                user=self.config.get(section, 'user'),
                password=self.config.get(section, 'password'),
                charset='utf8mb4',
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor
                )
        return conn

