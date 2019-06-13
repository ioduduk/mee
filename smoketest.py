#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import time
import os

import modules.remote as remote
import application.app as app
import utils.redisclient
import utils.kafkaclient

from elasticsearch import Elasticsearch
from application.config import config
from application.connection import ConnectinoPool

def testRedisConnection(result):
    host = config().get('redis', 'host')
    port = config().get('redis', 'port')
    db = config().get('redis', 'db')
    result['config']['redis'] = { 'host': host, 'port': port, 'db': db }

    startTime = time.time()

    try:
        client = utils.redisclient.RedisClient(host, port, db)
        pingResult = client.ping()
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        if pingResult:
            result['passed']['redis'] = { 'elapsed_ms': elapsedTime }
        else:
            result['code'] = 1001
            result['failed']['redis'] = { 'elapsed_ms': elapsedTime }
    except Exception as e:
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        result['code'] = 1001
        result['failed']['redis'] = { 'elapsed_ms': elapsedTime, 'message': str(e) }

def testKafkaConnection(result):
    host = config().get('kafka', 'host')
    result['config']['kafka'] = { 'host': host }

    startTime = time.time()

    try:
        producer = utils.kafkaclient.KafkaProducer(host)
        metadata = producer.list_topics()

        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)

        topicMetadata = metadata.topics
        topicName = config().get('kafka', 'topic')
        relayTopicName = config().get('kafka', 'relay_topic')
        if topicName in topicMetadata and relayTopicName in topicMetadata:
            result['passed']['kafka'] = { 'elapsed_ms': elapsedTime }
        else:
            result['failed']['kafka'] = { 'elapsed_ms': elapsedTime, 'message': 'topics NOT exist' }
    except Exception as e:
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        result['code'] = 1001
        result['failed']['kafka'] = { 'elapsed_ms': elapsedTime, 'message': str(e) }

def testElasticSearchConnection(result):
    host = config().get('elasticsearch', 'host')
    hosts = host.split(',')

    result['config']['elasticsearch'] = { 'host': host }

    startTime = time.time()

    try:
        client = Elasticsearch(hosts)
        pingResult = client.ping()
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        if pingResult:
            result['passed']['elasticsearch'] = { 'elapsed_ms': elapsedTime }
        else:
            result['failed']['elasticsearch'] = { 'elapsed_ms': elapsedTime, 'message': 'fail to ping' }
    except Exception as e:
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        result['code'] = 1001
        result['failed']['elasticsearch'] = { 'elapsed_ms': elapsedTime, 'message': str(e) }

def testMysqlConnection(result, configKey):
    host = config().get('mysql:' + configKey, 'host')
    port = config().get('mysql:' + configKey, 'port')
    db = config().get('mysql:' + configKey, 'database')

    result['config']['mysql:' + configKey] = { 'host': host, 'port': port, 'database': db }

    startTime = time.time()

    try:
        connPool = ConnectinoPool()
        conn = connPool.connection(configKey)
        with conn.cursor() as cursor:
            cursor.execute('show tables;')
            data = cursor.fetchall()

        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        result['passed']['mysql:' + configKey] = { 'elapsed_ms': elapsedTime }
    except Exception as e:
        endTime = time.time()
        elapsedTime = int((endTime-startTime) * 1000)
        result['code'] = 1001
        result['failed']['mysql:' + configKey] = { 'elapsed_ms': elapsedTime, 'message': str(e) }

if __name__ == '__main__':
    prjRoot = os.path.abspath(os.path.dirname(__file__))
    app.init(prjRoot + '/conf/app.ini')

    result = {
            'code': 0,
            'config': {},
            'passed': {},
            'failed': {}
            }

    testRedisConnection(result)
    testKafkaConnection(result)
    testElasticSearchConnection(result)
    testMysqlConnection(result, 'unittest')

    print(result)
