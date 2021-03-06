import os
from unittest import TestCase

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import create_engine

import pyro.tj
from tests.alchemy import DatabaseTestCase


class TestVectorSerialization(TestCase):
    def test_encode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        relations = [
            {'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
            {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}},
            {'name': 'R_3', 'attributes': {'A_3': 'INT', 'A_4': 'INT'}}]
        self.assertEqual(pyro.tj.encode_vector(relations),
                         "['A_1', 'A_2'],['A_2', 'A_3'],['A_3', 'A_4']")
        pyro.tj.VECTOR_SEPARATOR = '/'
        self.assertEqual(pyro.tj.encode_vector(relations),
                         "['A_1', 'A_2']/['A_2', 'A_3']/['A_3', 'A_4']")

    def test_decode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        pyro.tj.VECTOR_ATTRIBUTE = 'g'
        self.assertEqual(pyro.tj.decode_vector(
            {'g': 'users,addresses,payments,pictures'}),
            ['users', 'addresses', 'payments', 'pictures'])


class TestIsVectorLess(TestCase):
    def test(self):
        pyro.tj.VECTOR_SEPARATOR = ';'
        self.assertTrue(pyro.tj.is_vector_less({'g': '[A_1,A_2]'},
                                               {'g': '[A_1,A_2]'}))
        self.assertTrue(pyro.tj.is_vector_less({'g': '[A_1,A_2];[A_3,A_4]'},
                                               {'g': '[A_1,A_2];[A_3,A_4]'}))
        # 1 out of 2 relations
        self.assertTrue(pyro.tj.is_vector_less({'g': '[A_1,A_2]'},
                                               {'g': '[A_1,A_2];[A_3,A_4]'}))
        self.assertTrue(pyro.tj.is_vector_less({'g': '[A_3,A_4]'},
                                               {'g': '[A_1,A_2];[A_3,A_4]'}))
        # 2 out of 3 relations
        self.assertTrue(pyro.tj.is_vector_less(
            {'g': '[A_1,A_2];[A_3,A_4]'},
            {'g': '[A_1,A_2];[A_3,A_4];[A_4,A_5]'}))
        self.assertTrue(pyro.tj.is_vector_less(
            {'g': '[A_1,A_2];[A_4,A_5]'},
            {'g': '[A_1,A_2];[A_3,A_4];[A_4,A_5]'}))
        self.assertTrue(pyro.tj.is_vector_less(
            {'g': '[A_3,A_4];[A_4,A_5]'},
            {'g': '[A_1,A_2];[A_3,A_4];[A_4,A_5]'}))
        # different relations
        self.assertFalse(pyro.tj.is_vector_less(
            {'g': '[A_3,A_4];[A_4,A_5]'},
            {'g': '[A_1,A_2];[A_3,A_4];[A_4,A_5,A_6]'}))
        self.assertFalse(pyro.tj.is_vector_less({'g': '[A_3,A_4]'},
                                                {'g': '[A_1,A_2]'}))
        # excess relations
        self.assertFalse(pyro.tj.is_vector_less(
            {'g': '[A_1,A_2];[A_3,A_4];[A_4,A_5,A_6]'},
            {'g': '[A_1,A_2];[A_3,A_4]'}))


class TestIsSubordinate(TestCase):
    def test_same_relation_set_one_relation(self):
        self.assertTrue(
            pyro.tj.is_subordinate({'A_1': 'a', 'A_2': 'b', 'g': '[A_1,A_2]'},
                                   {'A_1': 'a', 'A_2': 'b', 'g': '[A_1,A_2]'}))
        self.assertFalse(
            pyro.tj.is_subordinate({'A_1': 'aa', 'A_2': 'b', 'g': '[A_1,A_2]'},
                                   {'A_1': 'a', 'A_2': 'b', 'g': '[A_1,A_2]'}))
        self.assertFalse(
            pyro.tj.is_subordinate({'A_1': None, 'A_2': 'b', 'g': '[A_1,A_2]'},
                                   {'A_1': 'a', 'A_2': 'b', 'g': '[A_1,A_2]'}))
        self.assertFalse(
            pyro.tj.is_subordinate({'A_1': 'a', 'A_2': 'b', 'g': '[A_1,A_2]'},
                                   {'A_1': 'a', 'A_2': None,
                                    'g': '[A_1,A_2]'}))

    def test_same_relation_set_two_relations(self):
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        vector = '[A_1,A_2],[A_2,A_3]'
        self.assertTrue(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': vector},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': vector}))
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': vector},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': vector}))
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': vector},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': vector}))
        self.assertTrue(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': vector},
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': vector}))

    def test_different_relation_sets(self):
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'},
            {'A_1': None, 'A_2': 'b', 'A_3': 'c', 'g': '[A_2,A_3]'}))
        self.assertTrue(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': '[A_1,A_2],[A_2,A_3]'}))
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': None, 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': '[A_1,A_2],[A_2,A_3]'}))
        self.assertTrue(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2],[A_2,A_3]'}))
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2],[A_2,A_3]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'}))

        # same attribute values but different relation set, which leads to non
        # subordination
        self.assertFalse(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'A_4': None,
             'g': '[A_1,A_2]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'A_4': 'd',
             'g': '[A_1,A_3],[A_2,A_4]'}))

    def test_missing_attributes(self):
        """
        Check function behavior in case not all attributes are present in
        each row. This is the case when calling the function with existing join
        data and new rows that don't have all the attributes set.
        """
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}},
                   {'name': 'R_3', 'attributes': {'A_3': 'INT', 'A_4': 'INT'}}]
        self.assertTrue(pyro.tj.is_subordinate(
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'A_4': None,
             'g': '[A_1,A_2]'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': '[A_1,A_2],[A_2,A_3]'}))


class TestFilterSubordinateRows(TestCase):
    def test(self):
        base_row_1 = {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'}
        base_row_2 = {'A_1': 'a', 'A_2': None, 'A_3': None, 'g': '[A_1,A_2]'}
        base_row_3 = {'A_1': None, 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2]'}
        tj_data = [base_row_1, base_row_2, base_row_3]
        new_data = [
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': '[A_1,A_2],[A_2,A_3]'},
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': '[A_1,A_2],[A_2,A_3]'}]

        rows_to_delete = pyro.tj.filter_subordinate_rows(tj_data, new_data)

        self.assertEqual(next(rows_to_delete), base_row_1)
        self.assertEqual(next(rows_to_delete), base_row_2)
        self.assertRaises(StopIteration)


class TestBuild(DatabaseTestCase):
    def setUp(self):
        self.cache_file_path = 'cache.json'
        super(TestBuild, self).setUp()

    def tearDown(self):
        os.remove(self.cache_file_path)
        super(TestBuild, self).tearDown()

    def test_relations_order_no_cache(self):
        """
        Context should be treated in the same way regardless of its relations
        order
        """
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
        dependencies = []
        constraint = []
        source = self.engine
        cube = create_engine('sqlite://')  # additional in-memory DB for test
        metadata = MetaData(source, reflect=True)
        Table('R1', metadata,
              Column('A', Integer, primary_key=True),
              Column('B', Integer, primary_key=True),
              Column('C', Integer))
        Table('R2', metadata,
              Column('C', Integer, primary_key=True),
              Column('D', Integer))
        metadata.create_all()

        tj_1 = pyro.tj.build([r1, r2], dependencies, constraint, source, cube,
                             self.cache_file_path)
        os.remove(self.cache_file_path)  # clear cache file
        tj_2 = pyro.tj.build([r2, r1], dependencies, constraint, source, cube,
                             self.cache_file_path)

        metadata = MetaData(cube, reflect=True)
        self.assertEqual(len(metadata.tables.keys()), 2)
        self.assertIn(tj_1['name'], metadata.tables)
        self.assertIn(tj_2['name'], metadata.tables)

    def test_integration_empty_tables_no_cache(self):
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
        constraint = []
        dependencies = [{'left': {'C'}, 'right': {'D'}}]
        source = self.engine
        cube = create_engine('sqlite://')  # additional in-memory DB for test
        metadata = MetaData(source, reflect=True)
        Table('R1', metadata,
              Column('A', Integer, primary_key=True),
              Column('B', Integer, primary_key=True),
              Column('C', Integer))
        Table('R2', metadata,
              Column('C', Integer, primary_key=True),
              Column('D', Integer))
        metadata.create_all()

        tj_1 = pyro.tj.build([r1, r2], dependencies, constraint, source, cube,
                             self.cache_file_path)

        metadata = MetaData(cube, reflect=True)
        self.assertEqual(len(metadata.tables.keys()), 1)
        self.assertIn(tj_1['name'], metadata.tables)
        with cube.connect() as conn:
            s = metadata.tables[tj_1['name']].select()
            res = conn.execute(s)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 0)

    def test_integration_with_data_no_cache(self):
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
        constraint = []
        dependencies = [{'left': {'C'}, 'right': {'D'}}]
        source = self.engine
        cube = create_engine('sqlite://')  # additional in-memory DB for test
        metadata = MetaData(source, reflect=True)
        t1 = Table('R1', metadata,
                   Column('A', Integer, primary_key=True),
                   Column('B', Integer, primary_key=True),
                   Column('C', Integer))
        t2 = Table('R2', metadata,
                   Column('C', Integer, primary_key=True),
                   Column('D', Integer))
        metadata.create_all()
        # populate with data
        with source.connect() as conn:
            conn.execute(t1.insert(), [
                {'A': 1, 'B': 2, 'C': 3},
                {'A': 1, 'B': 22, 'C': None}
            ])
            conn.execute(t2.insert(), [
                {'C': 3, 'D': 4},
                {'C': 33, 'D': 44}
            ])

        pyro.tj.VECTOR_SEPARATOR = ','
        tj = pyro.tj.build([r1, r2], dependencies, constraint, source, cube,
                           self.cache_file_path)

        metadata = MetaData(cube, reflect=True)
        self.assertIn(tj['name'], metadata.tables)
        with cube.connect() as conn:
            s = metadata.tables[tj['name']].select()
            res = conn.execute(s)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 3)
        self.assertEqual(all_records[0]['A'], 1)
        self.assertEqual(all_records[0]['B'], 22)
        self.assertEqual(all_records[0]['C'], None)
        self.assertEqual(all_records[0]['D'], None)
        self.assertEqual(all_records[0]['g'], "['A', 'B', 'C']")

        self.assertEqual(all_records[1]['A'], None)
        self.assertEqual(all_records[1]['B'], None)
        self.assertEqual(all_records[1]['C'], 33)
        self.assertEqual(all_records[1]['D'], 44)
        self.assertEqual(all_records[1]['g'], "['C', 'D']")

        self.assertEqual(all_records[2]['A'], 1)
        self.assertEqual(all_records[2]['B'], 2)
        self.assertEqual(all_records[2]['C'], 3)
        self.assertEqual(all_records[2]['D'], 4)
        self.assertEqual(all_records[2]['g'], "['A', 'B', 'C'],['C', 'D']")

    def test_integration_with_data_and_constraint_no_cache(self):
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
        constraint = [
            [{'attribute': 'D', 'operation': '>', 'value': 40}],
            [{'attribute': 'D', 'operation': '=', 'value': 37.5}]
        ]
        dependencies = [{'left': {'C'}, 'right': {'D'}}]
        source = self.engine
        cube = create_engine('sqlite://')  # additional in-memory DB for test
        metadata = MetaData(source, reflect=True)
        t1 = Table('R1', metadata,
                   Column('A', Integer, primary_key=True),
                   Column('B', Integer, primary_key=True),
                   Column('C', Integer))
        t2 = Table('R2', metadata,
                   Column('C', Integer, primary_key=True),
                   Column('D', Integer))
        metadata.create_all()
        # populate with data
        with source.connect() as conn:
            conn.execute(t1.insert(), [
                {'A': 1, 'B': 2, 'C': 3},
                {'A': 1, 'B': 22, 'C': None}
            ])
            conn.execute(t2.insert(), [
                {'C': 3, 'D': 4},
                {'C': 33, 'D': 44}
            ])

        tj = pyro.tj.build([r1, r2], dependencies, constraint, source, cube,
                           self.cache_file_path)

        metadata = MetaData(cube, reflect=True)
        self.assertIn(tj['name'], metadata.tables)
        with cube.connect() as conn:
            s = metadata.tables[tj['name']].select()
            res = conn.execute(s)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 2)
        self.assertEqual(all_records[0]['A'], 1)
        self.assertEqual(all_records[0]['B'], 22)
        self.assertEqual(all_records[0]['C'], None)
        self.assertEqual(all_records[0]['D'], None)
        self.assertEqual(all_records[0]['g'], "['A', 'B', 'C']")

        self.assertEqual(all_records[1]['A'], None)
        self.assertEqual(all_records[1]['B'], None)
        self.assertEqual(all_records[1]['C'], 33)
        self.assertEqual(all_records[1]['D'], 44)
        self.assertEqual(all_records[1]['g'], "['C', 'D']")

    def test_pseudocontext_no_cache(self):
        """
        Check that in case with pseudocontexts the whole relation set is used
        to construct TJ
        """
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
        constraint = []
        dependencies = []
        source = self.engine
        cube = create_engine('sqlite://')  # additional in-memory DB for test
        metadata = MetaData(source, reflect=True)
        t1 = Table('R1', metadata,
                   Column('A', Integer, primary_key=True),
                   Column('B', Integer, primary_key=True),
                   Column('C', Integer))
        t2 = Table('R2', metadata,
                   Column('C', Integer, primary_key=True),
                   Column('D', Integer))
        metadata.create_all()
        # populate with data
        with source.connect() as conn:
            conn.execute(t1.insert(), [
                {'A': 1, 'B': 2, 'C': 3},
                {'A': 1, 'B': 22, 'C': None}
            ])
            conn.execute(t2.insert(), [
                {'C': 3, 'D': 4},
                {'C': 33, 'D': 44}
            ])

        tj = pyro.tj.build([r1, r2], dependencies, constraint, source, cube,
                           self.cache_file_path)

        metadata = MetaData(cube, reflect=True)
        self.assertEqual(len(metadata.tables.keys()), 1)
        self.assertIn(tj['name'], metadata.tables)
        with cube.connect() as conn:
            s = metadata.tables[tj['name']].select()
            res = conn.execute(s)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 3)
        self.assertIn("['A', 'B', 'C'],['C', 'D']",  # R1 & R2 vector
                      map(lambda r: r['g'], all_records))
