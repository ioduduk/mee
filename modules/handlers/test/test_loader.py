# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
import yaml

from ..loader import Loader

class YamlIncludeTest(unittest.TestCase):
    def test_yaml_include(self):
        data = yaml.load(open('conf/handlers/config.yml'), utils.yaml_loader.Loader)
        x = {}
        for i in data:
            x.update(i)
        print(x)

