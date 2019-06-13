#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import unittest

from application import app
from application import config

if __name__ == '__main__':
    app.init('./conf/unittest.ini')

    topLevelDir = os.path.abspath(os.path.dirname(__file__) + '/../')
    startDir = topLevelDir
    if len(sys.argv) >= 2:
        startDir = topLevelDir + '/' + sys.argv[1]

    if os.path.isfile(startDir):
        testDir = os.path.dirname(startDir)
        fileName = os.path.basename(startDir)
        discover = unittest.defaultTestLoader.discover(testDir, pattern=fileName, top_level_dir=topLevelDir)
    else:
        if len(sys.argv) >= 3:
            pattern = sys.argv[2]
        else:
            pattern = 'test*.py'

        discover = unittest.defaultTestLoader.discover(startDir, pattern=pattern, top_level_dir=topLevelDir)

    runner = unittest.TextTestRunner()
    runner.run(discover)


