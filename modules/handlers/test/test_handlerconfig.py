# -*- coding: utf-8 -*-

from __future__ import print_function, division

import unittest
import time
import random
import yaml
import simplejson as json

import application.app as app
import modules.remote as remote

from ..handlerconfig import _DEPENDENCE_RE
from ..handlerconfig import *

class HandlerConfigTests(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        handlerConfig = HandlerConfig()
        handlerConfig.loadFromFile('./modules/handlers/test/conf/test.yml')
        self.handlerConfig = handlerConfig

    @classmethod
    def tearDownClass(self):
        pass

    def test_load_failed(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'the config file(.*?) MUST be valid', handlerConfig.loadFromFile, './modules/handlers/test/conf/not_exist.yml')
        self.assertRaisesRegexp(IllegalConfigException, 'the jsonData string MUST be jsonable', handlerConfig.loadFromJson, 'not a jsonable string')

    def test_dependence_re(self):
        statement = "select * from table1 where id = %t1.id and %%t2.tid" 
        result = _DEPENDENCE_RE.findall(statement)

    def test_load_to_json(self):
        jsonStr = self.handlerConfig.toJson()
        data = json.loads(jsonStr)

        config = HandlerConfig()
        config.loadFromJson(jsonStr)

        jsonStr1 = config.toJson()
        data1 = json.loads(jsonStr1)

        self.assertDictEqual(data, data1)

    def test_indices(self):
        indices = self.handlerConfig.indices()
        self.assertListEqual(indices, ['index_unittest'])

    def test_types(self):
        types = self.handlerConfig.types('index_unittest')
        self.assertListEqual(types, ['captain'])

        types = self.handlerConfig.types('non_exist')
        self.assertListEqual(types, [])

    def test_getConfigListByIndexAndType(self):
        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        self.assertIsInstance(configList, HandlerConfigList)

        configList = self.handlerConfig.getConfigListByIndexAndType('non_exist', 'doc')
        self.assertIsNone(configList)

    def test_getConfigItemsByDatabaseAndTable(self):
        items = self.handlerConfig.getConfigItemsByDatabaseAndTable('index_unittest', 'doc')
        self.assertEqual(len(items), 2)
        self.assertIsInstance(items[0], HandlerConfigItem)

        items = self.handlerConfig.getConfigItemsByDatabaseAndTable('db_unitest', 'test')
        self.assertEqual(len(items), 1)
        self.assertTrue(items[0].isNested())

    def test_getmaster(self):
        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        masterItem = configList.getMasterItem()
        masterKey = configList.getMasterKey()
        self.assertTrue(masterItem.isMaster)
        self.assertEqual(masterItem['key'], 'users')
        self.assertEqual(masterKey, 'users')

    def test_get_slave_items(self):
        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        slaveItems = configList.getSlaveItems()
        self.assertEqual(len(slaveItems), 10)
        
    def test_getDependencesItems(self):
        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        items = configList.getDependentItems('users')
        self.assertEqual(len(items), 10)

        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        items = configList.getDependentItems('users', ['non_exist_field'])
        self.assertEqual(len(items), 0)

        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        items = configList.getDependentItems('users', ['id'])
        self.assertEqual(len(items), 10)

    def test_getNestedDependents(self):
        configList = self.handlerConfig.getConfigListByIndexAndType('index_unittest', 'doc')
        item = configList.getConfigItemByKey('test')
        result = item.getNestedDependentLists()
        self.assertEqual(len(result), 1)
        result = item.getNestedDependentLists(['non_exist'])
        self.assertEqual(len(result), 0)
        result = item.getNestedDependentLists(['id'])
        self.assertEqual(len(result), 1)

        item = configList.getConfigItemByKey('identity')
        result = item.getNestedDependentLists()
        self.assertEqual(len(result), 0)


    def test_dup_master_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'Duplicated master', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_dup_master.yml')

    def test_dup_key_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'Duplicated key', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_dup_key.yml')

    def test_loop_depend_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'Loop Dependence detected', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_loop_depend.yml')

    def test_too_many_depend_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'dependence count can NOT be greater than one', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_too_many_depend.yml')

    def test_nested_nested_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'NestedHandlerConfigList must NOT contain any child NestedHandlerConfigList', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_nested_nested.yml')

    def test_mapping_error(self):
        handlerConfig = HandlerConfig()
        self.assertRaisesRegexp(IllegalConfigException, 'mapping values MUST be dict or string', handlerConfig.loadFromFile, './modules/handlers/test/conf/invalid_mapping.yml')






