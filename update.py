#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, division

import sys
import os
import time
import argparse
import hashlib

import application.app as app

def validate_parser():
    parser = argparse.ArgumentParser(description='全量更新ES中的所有数据')
    parser.add_argument('-c', '--config', type=str, action='store', required=True, help='指定的配置文件。可以是绝对路径或者相对路径。若是相对路劲，则是相对于目录 <project root dir>/conf/handlers/')
    parser.add_argument('-n', '--name', type=str, action='store', required=True, help='指定的唯一任务名，必选，对应的增量更新进程需要指定同样的值。')
    parser.add_argument('-a', '--action', type=str, action='store', default='update', help='指定的动作，默认是update。目前只支持update、reset和clean: update, 全量更新；reset, 重置任务状态，通常在全量更新失败执行reset；clean, 清理脏数据，脏数据通常是因为全量更新失败造成的。')
    return parser

if __name__ == '__main__':
    prjRoot = os.path.abspath(os.path.dirname(__file__))
    app.init(prjRoot + '/conf/app.ini')

    argParser = validate_parser()
    args = argParser.parse_args()
    if os.path.isabs(args.config):
        handlerConfigPath = args.config
    else:
        handlerConfigPath = prjRoot + '/conf/handlers/' + args.config

    action = args.action
    name = args.name

    print('action is %s ' % action)
    print('handler config path is %s' % handlerConfigPath)
    print('task name is %s' % name)

    # 要在app.init后再import，这样可以使得logger生效
    from services.updateservice import UpdateService
    svc = UpdateService(name, handlerConfigPath)

    startTime = time.time()

    if action == 'update':
        result = svc.update()

        print("\n")
        if result:
            print('update successfully')
        else:
            print('update failed')
    elif action == 'reset':
        svc.reset()
    elif action == 'clean':
        svc.clean()
    else:
        print("param of action ONLY support 'update' or 'reset' or 'clean'", file=sys.stderr)
        print("查看帮助信息：\npython update.py -h\n")
        sys.exit(127)

    endTime = time.time()

    print("\n")
    print("-" * 50)
    print("Run with time cost %f seconds" % (endTime - startTime))
    print("\n" * 2)

