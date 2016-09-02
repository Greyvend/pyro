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
        self.assertEqual(pyro.tj.encode_vector([{'name': 'users'},
                                                {'name': 'addresses'},
                                                {'name': 'payments'},
                                                {'name': 'pictures'}]),
                         'users,addresses,payments,pictures')
        pyro.tj.VECTOR_SEPARATOR = '/'
        self.assertEqual(pyro.tj.encode_vector([{'name': 'users'},
                                                {'name': 'addresses'},
                                                {'name': 'payments'},
                                                {'name': 'pictures'}]),
                         'users/addresses/payments/pictures')

    def test_decode_vector(self):
        pyro.tj.VECTOR_SEPARATOR = ','
        pyro.tj.VECTOR_ATTRIBUTE = 'g'
        self.assertEqual(pyro.tj.decode_vector(
            {'g': 'users,addresses,payments,pictures'}),
            ['users', 'addresses', 'payments', 'pictures'])


class TestIsLessOrEqual(TestCase):
    def test_same_relation_set(self):
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'second', 'third'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'third', 'second'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['third', 'second', 'first'],
                                                 ['first', 'second', 'third']))

    def test_different_relation_set(self):
        # case 1: 1 out of 3 relations
        self.assertTrue(pyro.tj.is_less_or_equal(['first'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['third'],
                                                 ['first', 'second', 'third']))
        # case 2: 2 out of 3 relations
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'second'],
                                                 ['first', 'second', 'third']))
        self.assertTrue(pyro.tj.is_less_or_equal(['first', 'third'],
                                                 ['first', 'second', 'third']))
        # case 3: different relations
        self.assertFalse(pyro.tj.is_less_or_equal(
            ['first', 'second', 'fourth'], ['first', 'second', 'third']))
        self.assertFalse(pyro.tj.is_less_or_equal(['fourth'], ['fifth']))
        # case 4: excess relations
        self.assertFalse(pyro.tj.is_less_or_equal(
            ['first', 'second', 'third', 'fourth'],
            ['first', 'second', 'third']))


class TestIsSubordinate(TestCase):
    def test_same_relation_set_one_relation(self):
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}}]
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'aa', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': None, 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'g': 'R_1'},
            {'A_1': 'a', 'A_2': None, 'g': 'R_1'}))

    def test_same_relation_set_two_relations(self):
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'bb', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'}))

    def test_different_relation_sets(self):
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': None, 'A_2': 'b', 'A_3': 'c', 'g': 'R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'}))
        self.assertTrue(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1,R_2'}))
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'}))

        # same attribute values but different relation set, which leads to non
        # subordination
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_1': 'INT', 'A_3': 'INT'}},
                   {'name': 'R_3', 'attributes': {'A_2': 'INT', 'A_4': 'INT'}}]
        self.assertFalse(pyro.tj.is_subordinate(
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'A_4': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'A_4': 'd', 'g': 'R_2,R_3'})
        )

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
            context,
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'A_4': None, 'g': 'R_1'},
            {'A_1': 'a', 'A_2': 'b', 'A_3': 'c', 'g': 'R_1,R_2'}))


class TestFilterSubordinateRows(TestCase):
    def test(self):
        context = [{'name': 'R_1', 'attributes': {'A_1': 'INT', 'A_2': 'INT'}},
                   {'name': 'R_2', 'attributes': {'A_2': 'INT', 'A_3': 'INT'}}]
        base_row_1 = {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1'}
        base_row_2 = {'A_1': 'a', 'A_2': None, 'A_3': None, 'g': 'R_1'}
        base_row_3 = {'A_1': None, 'A_2': 'b', 'A_3': None, 'g': 'R_1'}
        tj_data = [base_row_1, base_row_2, base_row_3]
        new_data = [
            {'A_1': 'a', 'A_2': 'b', 'A_3': None, 'g': 'R_1,R_2'},
            {'A_1': 'a', 'A_2': None, 'A_3': 'c', 'g': 'R_1,R_2'}]

        rows_to_delete = pyro.tj.filter_subordinate_rows(context, tj_data,
                                                         new_data)

        self.assertEqual(next(rows_to_delete), base_row_1)
        self.assertEqual(next(rows_to_delete), base_row_2)
        self.assertRaises(StopIteration)


class TestBuild(DatabaseTestCase):
    def test_integration_empty_tables(self):
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
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

        pyro.tj.build([r1, r2], dependencies, source, cube)

        metadata = MetaData(cube, reflect=True)
        self.assertEqual(len(metadata.tables.keys()), 1)
        self.assertIn('TJ_R1_R2', metadata.tables)
        with cube.connect() as conn:
            tj = metadata.tables['TJ_R1_R2'].select()
            res = conn.execute(tj)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 0)

    def test_integration_with_data(self):
        r1 = {'name': 'R1', 'attributes': {'A': Integer, 'B': Integer,
                                           'C': Integer},
              'pk': {'A', 'B'}}
        r2 = {'name': 'R2', 'attributes': {'C': Integer, 'D': Integer},
              'pk': {'C'}}
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

        pyro.tj.build([r1, r2], dependencies, source, cube)

        metadata = MetaData(cube, reflect=True)
        self.assertIn('TJ_R1_R2', metadata.tables)
        with cube.connect() as conn:
            tj = metadata.tables['TJ_R1_R2'].select()
            res = conn.execute(tj)
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 3)
        self.assertEqual(all_records[0]['A'], 1)
        self.assertEqual(all_records[0]['B'], 22)
        self.assertEqual(all_records[0]['C'], None)
        self.assertEqual(all_records[0]['D'], None)
        self.assertEqual(all_records[0]['g'], 'R1')

        self.assertEqual(all_records[1]['A'], None)
        self.assertEqual(all_records[1]['B'], None)
        self.assertEqual(all_records[1]['C'], 33)
        self.assertEqual(all_records[1]['D'], 44)
        self.assertEqual(all_records[1]['g'], 'R2')

        self.assertEqual(all_records[2]['A'], 1)
        self.assertEqual(all_records[2]['B'], 2)
        self.assertEqual(all_records[2]['C'], 3)
        self.assertEqual(all_records[2]['D'], 4)
        self.assertEqual(all_records[2]['g'], 'R1,R2')
