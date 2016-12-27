import json
import os
from os.path import abspath, join, dirname
from shutil import copyfile
from unittest.mock import patch

from pyro.cache import Cache
from tests.alchemy import DatabaseTestCase


class TestCache(DatabaseTestCase):
    def setUp(self):
        sample_cache_file_path = abspath(join(dirname(__file__), 'test_data',
                                              'cache.json'))
        self.cache_file_path = abspath(join(dirname(__file__), 'test_data',
                                            'cache-tmp.json'))
        copyfile(sample_cache_file_path, self.cache_file_path)

    def tearDown(self):
        os.remove(self.cache_file_path)

    @patch('pyro.cache.is_domain_included', autospec=True)
    def test_enable_context_subordination(self, mock_domain_included):
        mock_domain_included.return_value = True
        r1 = {
            "name": "R_1", "attributes": {"A11": "Integer", "A12": "String",
                                          "A13": "Integer", "A14": "Boolean"}
        }
        r2 = {
            "name": "R_2", "attributes": {"A21": "Integer", "A22": "String",
                                          "A23": "Integer", "A24": "Boolean"}
        }
        r3 = {'name': 'R3', 'attributes': {'B': 'INT', 'D': 'INT'}}
        constraint = None
        engine = None

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r2], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r2, r1], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2, r3], constraint)

        self.assertFalse(cache.enabled)

    def test_enable_domain_contained(self):
        r1 = {
            "name": "R_1", "attributes": {"A11": "Integer", "A12": "String",
                                          "A13": "Integer", "A14": "Boolean"}
        }
        r2 = {
            "name": "R_2", "attributes": {"A21": "Integer", "A22": "String",
                                          "A23": "Integer", "A24": "Boolean"}
        }
        constraint = [[
            {
                "attribute": "A1",
                "operation": ">",
                "value": 7
            },
            {
                "attribute": "A2",
                "operation": "=",
                "value": 6
            }
        ]]
        engine = None

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2], constraint)

        self.assertTrue(cache.enabled)

    def test_enable_domain_not_contained(self):
        r1 = {
            "name": "R_1", "attributes": {"A11": "Integer", "A12": "String",
                                          "A13": "Integer", "A14": "Boolean"}
        }
        r2 = {
            "name": "R_2", "attributes": {"A21": "Integer", "A22": "String",
                                          "A23": "Integer", "A24": "Boolean"}
        }
        constraint = [[
            {
                "attribute": "A4",
                "operation": "=",
                "value": "str value"
            }
        ]]
        engine = None

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2], constraint)

        self.assertFalse(cache.enabled)

    def test_add_existing(self):
        relation = {
            "name": "TJ_1",
            "attributes": {
                "A1": "Integer",
                "A2": "String",
                "A4": "Integer",
                "A6": "String"
            }
        }
        context = [
            {
                "name": "R_1", "attributes": {"A11": "Integer",
                                              "A12": "String",
                                              "A13": "Integer",
                                              "A14": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A21": "Integer",
                                              "A22": "String",
                                              "A23": "Integer",
                                              "A24": "Boolean"}
            }]
        constraint = [
            [
                {
                    "attribute": "A1",
                    "operation": ">",
                    "value": 4
                },
                {
                    "attribute": "A2",
                    "operation": "=",
                    "value": 6
                }
            ],
            [
                {
                    "attribute": "A4",
                    "operation": "<>",
                    "value": "str value"
                },
                {
                    "attribute": "A6",
                    "operation": "NOT BETWEEN",
                    "value": [1, 16]
                }
            ]
        ]
        engine = None

        with open(self.cache_file_path) as config_file:
            old_config = json.load(config_file)

        cache = Cache(engine, self.cache_file_path)
        cache.add(relation, context, constraint)

        with open(self.cache_file_path) as config_file:
            new_config = json.load(config_file)
        self.assertEqual(old_config, new_config)

    def test_add_domain_subset(self):
        relation = {
            "name": "TJ_1",
            "attributes": {
                "A1": "Integer",
                "A2": "String",
                "A4": "Integer",
                "A6": "String"
            }
        }
        context = [
            {
                "name": "R_1", "attributes": {"A11": "Integer",
                                              "A12": "String",
                                              "A13": "Integer",
                                              "A14": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A21": "Integer",
                                              "A22": "String",
                                              "A23": "Integer",
                                              "A24": "Boolean"}
            }]
        constraint = [
            [
                {
                    "attribute": "A1",
                    "operation": "=",
                    "value": 111
                },
                {
                    "attribute": "A2",
                    "operation": "=",
                    "value": 6
                }
            ]
        ]
        engine = None

        with open(self.cache_file_path) as config_file:
            old_config = json.load(config_file)

        cache = Cache(engine, self.cache_file_path)
        cache.add(relation, context, constraint)

        with open(self.cache_file_path) as config_file:
            new_config = json.load(config_file)
        self.assertEqual(old_config, new_config)

    def test_add_new_entry(self):
        relation = {
            "name": "TJ_2",
            "attributes": {
                "A1": "Integer",
                "A2": "String"
            }
        }
        context = [
            {
                "name": "R_1", "attributes": {"A11": "Integer",
                                              "A12": "String",
                                              "A13": "Integer",
                                              "A14": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A21": "Integer",
                                              "A22": "String",
                                              "A23": "Integer",
                                              "A24": "Boolean"}
            }]
        constraint = [[{
            "attribute": "A1",
            "operation": "=",
            "value": 111
        }]]
        engine = None

        with open(self.cache_file_path) as config_file:
            old_config = json.load(config_file)

        cache = Cache(engine, self.cache_file_path)
        cache.add(relation, context, constraint)

        with open(self.cache_file_path) as config_file:
            new_config = json.load(config_file)
        self.assertNotEqual(old_config, new_config)
        self.assertIn(relation, [entry['relation'] for entry in new_config])
        self.assertIn(context, [entry['context'] for entry in new_config])
        self.assertIn(constraint, [entry['constraint']
                                   for entry in new_config])
