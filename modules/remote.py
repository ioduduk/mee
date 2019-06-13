# -*- coding: utf-8 -*-

from __future__ import print_function, division

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch_dsl import Search, Q
from elasticsearch.helpers import scan, bulk

import utils.redisclient
import utils.kafkaclient

from utils.singleton import cache
from application.config import config

_redis_client = None
def getRedisClient():
    global _redis_client

    if _redis_client is None:
        host = config().get('redis', 'host')
        port = config().get('redis', 'port')
        db = config().get('redis', 'db')
        _redis_client = utils.redisclient.RedisClient(host, port, db)
    
    return _redis_client

def getKafkaProducer():
    host = config().get('kafka', 'host')
    return utils.kafkaclient.KafkaProducer(host)

def getKafkaConsumer(groupId, autoCommit=True, autoOffsetReset='earliest'):
    host = config().get('kafka', 'host')
    return utils.kafkaclient.KafkaConsumer(
            groupId, host, autoCommit=autoCommit, autoOffsetReset=autoOffsetReset
            )


def getElasticClient():
    host = config().get('elasticsearch', 'host')
    hosts = host.split(',')
    return Elasticsearch(hosts)


