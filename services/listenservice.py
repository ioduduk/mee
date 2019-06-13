# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import os
import time
import datetime
import simplejson as json

import application.app as app
import modules.remote as remote
import utils.timeutil as timeutil

from datetime import datetime

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
        DeleteRowsEvent,
        UpdateRowsEvent,
        WriteRowsEvent,
        )
from pymysqlreplication.event import (
        QueryEvent,
        )

from application import NotSupportedException
from application.config import config
from application.connection import ConnectinoPool
from utils.failure import Failure

_logger = app.getLogger('base')
_binlogLogger = app.getLogger('binlog')

class ListenService(object):
    """
    监听指定数据库的binlog，并将数据写入kafka队列
    """
    def __init__(self, database):
        self.topic = config().get('kafka', 'topic')
        self.database = database

        self._connPool = ConnectinoPool()
        self._runPath = app.getPrjRoot() + "/run"
        self._binlogPosFile = self._runPath + "/" + database + "_collector_position.safe"

        self._kafkaProducer = self._initKafkaProducer()
        self._kafkaMsgRedeliveryCount = 0

        self._posStream = None
        self._stream = None

    def position(self, force=False):
        database = self.database
        posFilePath = self._binlogPosFile

        if not force and os.path.isfile(posFilePath):
            return

        conn = self._connPool.connection(database)
        with conn.cursor() as cursor:
            sqlMasterStatus = "show master status"
            cursor.execute(sqlMasterStatus)
            resultMasterStatus = cursor.fetchone()
            
            sqlBinlogFormat = "show variables like 'binlog_format'"
            cursor.execute(sqlBinlogFormat)
            resultFormat = cursor.fetchone()

            sqlSlaveStatus = "show slave status"
            cursor.execute(sqlSlaveStatus)
            resultSlaveStatus = cursor.fetchone()

        if resultFormat['Value'] != 'ROW':
            raise NotSupportedException("The binlog format is NOT ROW but %s. We only support ROW now." % resultFormat['Value'])

        print("=" * 32)
        print("Database: %s" % database)
        print("File: %s, Position: %s, BinlogFormat: %s" % (resultMasterStatus['File'], resultMasterStatus['Position'], resultFormat['Value']))

        if resultSlaveStatus:
            print("Slave_IO_State: %s\nLast_Error: %s" % (slave_status_result['Slave_IO_State'], slave_status_result['Last_Error']))
        else:
            print("No slave status acquired")
        print("=" * 32)

        # 将Position写入run目录中            
        with open(posFilePath, 'w+') as f:
            f.write("%(File)s:%(Position)s" % resultMasterStatus)

    def listen(self):
        _logger.info("Start to listen to the binlog of %s" % self.database)    

        section = 'mysql:' + self.database
        mysqlSetting = {
                "host": config().get(section, "host"),
                "port": int(config().get(section, 'port')),
                "user": config().get(section, "user"),
                "password": config().get(section, "password"),
                }

        watchedDatabases = [self.database]

        # load last binlog reader position
        logFile, logPos, resumeStrem = self._loadLastBinlogPos()

        self._stream = BinLogStreamReader(
                connection_settings=mysqlSetting,
                server_id=int(config().get(section, "slaveid")),
                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
                blocking=True,
                resume_stream=resumeStrem,
                log_file=logFile,
                log_pos=logPos,
                )

        while True:
            refresh = False
            try:
                for binlogEvent in self._stream:
                    refresh = True
                    logFile, logPos = self._stream.log_file, self._stream.log_pos

                    # filter no watch database
                    if binlogEvent.schema not in watchedDatabases:
                        self._writeBinlogPos(logFile, logPos)
                        continue

                    binlog = {}
                    binlog['storage'] = 'mysql'
                    binlog['database'] = '%s' % binlogEvent.schema
                    binlog['table'] = '%s' % binlogEvent.table
                    binlog['timestamp'] = datetime.fromtimestamp(binlogEvent.timestamp).strftime('%Y-%m-%d %H:%M:%S')

                    for row in binlogEvent.rows:
                        if isinstance(binlogEvent, DeleteRowsEvent):
                            binlog['values'] = row['values']
                            binlog['type'] = 'DELETE'
                        elif isinstance(binlogEvent, UpdateRowsEvent):
                            binlog['before'] = row['before_values']
                            binlog['values'] = row['after_values']
                            binlog['type'] = 'UPDATE'
                        elif isinstance(binlogEvent, WriteRowsEvent):
                            binlog['values'] = row['values']
                            binlog['type'] = 'INSERT'

                        binlogRow = json.dumps(binlog, default=timeutil.dateHandler)
                        self._pushToKafka(binlogRow, binlog['database'], binlog['table'])

                    # after pushing binlog to kafka, update the binlog position
                    self._writeBinlogPos(logFile, logPos)

                if not refresh:
                    _logger.info("NO new input binlog, current position: [%s:%d]", logFile if logFile is not None else "", logPos if logPos is not None else 0)
                    time.sleep(0.1)
            except Exception as e:
                print(e)
                sys.exit(1)

    def _loadLastBinlogPos(self):
        if not os.path.exists(self._binlogPosFile):
            return (None, None, False)
        
        try:
            with open(self._binlogPosFile) as f:
                res = f.readline()
                if res.find(":") < 0:
                    return (None, None, False)

                logFile, logPos = res.split(":")
                logPos = int(logPos)
        except IOError:
            return (None, None, False)

        return (logFile, logPos, True)

    def _writeBinlogPos(self, logFile, logPos):
        _logger.info('locate binlog file[%s] position [%d]', logFile, logPos)
        if self._posStream is None:
            self._posStream = open(self._binlogPosFile, 'w')

        self._posStream.write("%s:%d" % (logFile, logPos))

    def _initKafkaProducer(self):
        try:
            kafkaProducer = remote.getKafkaProducer()
            return kafkaProducer
        except Exception as e:
            _logger.error("Fail to init a kafka producer")
            sys.exit(1)

    def _pushToKafka(self, rowValue, database, table):
        try:
            self._kafkaMsgRedeliveryCount = 0
            key = database + table
            self._kafkaProducer.produce(
                    self.topic, 
                    rowValue.encode("utf-8"), 
                    key,
                    callback=self._kafkaDeliveryCallback
                    )
            self._kafkaProducer.flush()
            return True
        except Exception as e:
            _logger.error("Fail to push to topic[%s] row[%s]. Error: %s", self.topic, rowValue, e)
            sys.exit(1)

    def _kafkaDeliveryCallback(self, err, msg):
        if err:
            if self._kafkaMsgRedeliveryCount < 3:
                """
                redelivery msg when error happens
                """
                self._kafkaMsgRedeliveryCount += 1
                _binlogLogger.info("redelivery(count=%d): topic[%s], msg[%s]", self._kafkaMsgRedeliveryCount, self.topic, msg.value())
                self._kafkaProducer.produce(self.topic, msg.value(), callback=self._kafkaDeliveryCallback)
            else:
                _binlogLogger.error("delivery failed; topic[%s], msg[%s], err[%s]", self.topic, msg.value(), err)
        else:
            _binlogLogger.info("topic[%s], msg[%s]", self.topic, msg.value())

    def __del__(self):
        if self._posStream:
            self._posStream.flush()
            self._posStream.close()
