#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
监听指定数据库的binlog
"""

from __future__ import print_function, division

import sys
import os
import time
import argparse

import application.app as app

def validate_parser():
    parser = argparse.ArgumentParser(description='监听指定MySQL的binlog，然后写入Kafka')
    parser.add_argument('-d', '--database', type=str, action='store', required=True, help='指定的要监听binlog的数据库')
    parser.add_argument('-f', '--force', action='store_true', help='是否强制更新存储在run目录下的binlog的position')
    return parser

if __name__ == '__main__':
    prjRoot = os.path.abspath(os.path.dirname(__file__))
    app.init(prjRoot + '/conf/app.ini')

    argParser = validate_parser()
    args = argParser.parse_args()

    database = args.database
    force = args.force
    print('database to listen to is %s' % database)
    print('force to update binlog positions? %s' % ('Y' if force else 'N'))

    # 要在app.init后再import，这样可以使得logger生效
    from services.listenservice import ListenService
    svc = ListenService(database=database)

    # 是否需要更新binlog的position
    svc.position(force=force)

    # 开始监听binlog
    print("Start to listen to the binlog of %s" % database)    
    svc.listen()

