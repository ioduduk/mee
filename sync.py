#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import os
import time
import argparse

import application.app as app

def validate_parser():
    parser = argparse.ArgumentParser(description='分发binlog events')
    parser.add_argument('-n', '--name', type=str, action='store', required=True, help='指定的唯一任务名，必选，对应的全量更新进程需要指定同样的值。')
    return parser

if __name__ == '__main__':
    prjRoot = os.path.abspath(os.path.dirname(__file__))
    app.init(prjRoot + '/conf/app.ini')

    argParser = validate_parser()
    args = argParser.parse_args()
    name = args.name

    print('task name is %s' % name)

    # 要在app.init后再import，这样可以使得logger生效
    from services.syncservice import SyncService
    svc = SyncService(name)

    svc.sync()


