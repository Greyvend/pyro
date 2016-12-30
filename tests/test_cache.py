import json
import os
from os.path import abspath, join, dirname
from shutil import copyfile
from unittest.mock import patch

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table

from pyro import db
from pyro.cache import Cache
from tests.alchemy import DatabaseTestCase


class TestCache(DatabaseTestCase):
    def setUp(self):
        sample_cache_file_path = abspath(join(dirname(__file__), 'test_data',
                                              'cache.json'))
        self.cache_file_path = abspath(join(dirname(__file__), 'test_data',
                                            'cache-tmp.json'))
        copyfile(sample_cache_file_path, self.cache_file_path)
        super(TestCache, self).setUp()

    def tearDown(self):
        os.remove(self.cache_file_path)
        super(TestCache, self).tearDown()

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

        self.assertFalse(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r2], constraint)

        self.assertFalse(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r2, r1], constraint)

        self.assertTrue(cache.enabled)

        cache = Cache(engine, self.cache_file_path)
        cache.enable([r1, r2, r3], constraint)

        self.assertTrue(cache.enabled)

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

    def test_contains_unsatisfied(self):
        metadata = MetaData(self.engine, reflect=True)
        tj_cached = Table('TJ_1', metadata,
                          Column('A1', Integer, primary_key=True),
                          Column('A2', String(20)),
                          Column('A4', Integer),
                          Column('A6', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            row_1 = {'A1': 1, 'A2': 'a12 str', 'A4': 14, 'A6': 'a16 str'}
            conn.execute(tj_cached.insert(), [
                row_1,
                {'A1': 2, 'A2': 'a22 str', 'A4': 24, 'A6': 'a26 str'}
            ])

        context = [
            {
                "name": "R_1", "attributes": {"A1": "Integer",
                                              "A2": "String",
                                              "A3": "Integer",
                                              "A4": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A1": "Integer",
                                              "A6": "String"}
            },
            {
                "name": "R_3", "attributes": {"A1": "Integer",
                                              "A7": "String"}
            }]
        constraint = []
        constraint_to_check = [[{
            "attribute": "A1",
            "operation": "IN",
            "value": [6, 8, 12]
        }]]
        engine = self.engine

        cache = Cache(engine, self.cache_file_path)
        cache._config[0]['constraint'] = constraint
        cache.enable(context=context, constraint=constraint)

        contains = cache.contains(constraint_to_check)

        self.assertFalse(contains)

    def test_contains_satisfied(self):
        metadata = MetaData(self.engine, reflect=True)
        tj_cached = Table('TJ_1', metadata,
                          Column('A1', Integer, primary_key=True),
                          Column('A2', String(20)),
                          Column('A4', Integer),
                          Column('A6', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            row_1 = {'A1': 1, 'A2': 'a12 str', 'A4': 14, 'A6': 'a16 str'}
            conn.execute(tj_cached.insert(), [
                row_1,
                {'A1': 2, 'A2': 'a22 str', 'A4': 24, 'A6': 'a26 str'}
            ])

        context = [
            {
                "name": "R_1", "attributes": {"A1": "Integer",
                                              "A2": "String",
                                              "A3": "Integer",
                                              "A4": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A1": "Integer",
                                              "A6": "String"}
            },
            {
                "name": "R_3", "attributes": {"A1": "Integer",
                                              "A7": "String"}
            }]
        constraint = []
        constraint_to_check = [[{
            "attribute": "A1",
            "operation": ">=",
            "value": 2
        }]]
        engine = self.engine

        cache = Cache(engine, self.cache_file_path)
        cache._config[0]['constraint'] = constraint
        cache.enable(context=context, constraint=constraint)

        contains = cache.contains(constraint_to_check)

        self.assertTrue(contains)

    def test_contains_context_unsatisfied(self):
        context = [
            {
                "name": "R_1", "attributes": {"A1": "Integer",
                                              "A2": "String",
                                              "A3": "Integer",
                                              "A4": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A1": "Integer",
                                              "A6": "String"}
            },
            {
                "name": "R_3", "attributes": {"A1": "Integer",
                                              "A7": "String"}
            }]
        constraint = []
        engine = self.engine

        cache = Cache(engine, self.cache_file_path)
        cache._config[0]['constraint'] = constraint
        cache._config[0]['context'] = context
        cache.enable(context=context, constraint=constraint)

        contains = cache.contains_context([{"name": "R_1"}, {"name": "R_5"}])

        self.assertFalse(contains)

    def test_contains_context_satisfied(self):
        context = [
            {
                "name": "R_1", "attributes": {"A1": "Integer",
                                              "A2": "String",
                                              "A3": "Integer",
                                              "A4": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A1": "Integer",
                                              "A6": "String"}
            },
            {
                "name": "R_3", "attributes": {"A1": "Integer",
                                              "A7": "String"}
            }]
        constraint = []
        engine = self.engine

        cache = Cache(engine, self.cache_file_path)
        cache._config[0]['constraint'] = constraint
        cache._config[0]['context'] = context
        cache.enable(context=context, constraint=constraint)

        contains = cache.contains_context([{"name": "R_2"}])
        self.assertTrue(contains)

        contains = cache.contains_context([{"name": "R_1"}, {"name": "R_2"}])
        self.assertTrue(contains)

        contains = cache.contains_context([{"name": "R_2"}, {"name": "R_3"}])
        self.assertTrue(contains)

        contains = cache.contains_context(context)
        self.assertTrue(contains)

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

    def test_restore(self):
        metadata = MetaData(self.engine, reflect=True)
        tj_cached = Table('TJ_1', metadata,
                          Column('A1', Integer, primary_key=True),
                          Column('A2', String(20)),
                          Column('A4', Integer),
                          Column('A6', String(50)))
        new_tj = Table('TJ_new', metadata,
                       Column('A1', Integer, primary_key=True),
                       Column('A2', String(20)),
                       Column('A4', Integer),
                       Column('A6', String(50)),
                       Column('A7', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            row_1 = {'A1': 1, 'A2': 'a12 str', 'A4': 14, 'A6': 'a16 str'}
            conn.execute(tj_cached.insert(), [
                row_1,
                {'A1': 2, 'A2': 'a22 str', 'A4': 24, 'A6': 'a26 str'}
            ])

        dest_relation = {
            "name": new_tj.name,
            "attributes": {
                "A1": "Integer",
                "A2": "String",
                "A4": "Integer",
                "A6": "String",
                "A7": "String"
            }
        }
        context = [
            {
                "name": "R_1", "attributes": {"A1": "Integer",
                                              "A2": "String",
                                              "A3": "Integer",
                                              "A4": "Boolean"}
            },
            {
                "name": "R_2", "attributes": {"A1": "Integer",
                                              "A6": "String"}
            },
            {
                "name": "R_3", "attributes": {"A1": "Integer",
                                              "A7": "String"}
            }]
        constraint = [[{
            "attribute": "A1",
            "operation": "<",
            "value": 2
        }]]
        engine = self.engine

        cache = Cache(engine, self.cache_file_path)
        cache._config[0]['constraint'] = constraint
        cache.enable(context=context, constraint=constraint)

        cache.restore(dest_relation, constraint)

        res = db.get_rows(self.engine, dest_relation)
        self.assertEqual(len(res), 1)
        first_row = res[0]
        del first_row['A7']
        self.assertDictEqual(dict(first_row), row_1)
