from sqlalchemy import MetaData, Integer, String, Column, Table, ForeignKey
from sqlalchemy.dialects.mysql import REAL
from sqlalchemy.exc import InternalError
from sqlalchemy.sql.sqltypes import _Binary, Text, LargeBinary, Float

from pyro import db
from pyro.db import create_table, _transform_column_type, _execute, \
    get_rows, delete_rows, insert_rows, get_data, delete_unsatisfied
from tests.alchemy import DatabaseTestCase


class TestFetchMechanism(DatabaseTestCase):
    def test(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])

            # case 1: all at once
            res = conn.execute(users.select())
            all_records = res.fetchall()

            # case 2: fetch rows one by one
            res = conn.execute(users.select())
            first_row = res.fetchone()
            second_row = res.fetchone()


class TestCreateTable(DatabaseTestCase):
    def test_new_table(self):
        metadata = MetaData(self.engine, reflect=True)
        relation = {'name': 'test_table', 'attributes': {'first': Integer,
                                                         'second': String(10)}}

        self.assertEqual(len(metadata.tables), 0)

        create_table(self.engine, relation)
        metadata.reflect()
        self.assertEqual(len(metadata.tables), 1)
        self.assertEqual(len(metadata.tables['test_table'].columns), 2)

    def test_idempotence(self):
        metadata = MetaData(self.engine, reflect=True)
        relation = {'name': 'test_table', 'attributes': {'first': Integer,
                                                         'second': String(10)}}

        self.assertEqual(len(metadata.tables), 0)

        create_table(self.engine, relation)
        create_table(self.engine, relation)

        metadata.reflect()
        self.assertEqual(len(metadata.tables), 1)

    def test_different_attributes(self):
        metadata = MetaData(self.engine, reflect=True)
        relation = {'name': 'test_table', 'attributes': {'first': Integer,
                                                         'second': String(10)}}

        self.assertEqual(len(metadata.tables), 0)

        create_table(self.engine, relation)
        relation['attributes'].pop('second')
        create_table(self.engine, relation)

        metadata.reflect()
        self.assertEqual(len(metadata.tables), 1)


class TestTransformColumnType(DatabaseTestCase):
    def test_handled_types(self):
        integer = Integer()
        _str = String(15)
        binary = _Binary()
        self.assertEqual(_transform_column_type(integer), Integer)
        self.assertEqual(_transform_column_type(_str), Text)
        self.assertEqual(_transform_column_type(binary), LargeBinary)

    def test_unhandled_type(self):
        real = REAL()

        transformed_type = _transform_column_type(real)
        self.assertIsNone(transformed_type.collation)
        self.assertEqual(transformed_type.encoding, 'utf-8')


class TestExecute(DatabaseTestCase):
    def test_single_row_result(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])

            res = _execute(self.engine, users.select())

        first_row = res[0]
        self.assertEqual(first_row['user_name'], 'jack')
        self.assertEqual(first_row['user_fullname'], 'Jack Jones')
        second_row = res[1]
        self.assertEqual(second_row['user_name'], 'wendy')
        self.assertEqual(second_row['user_fullname'], 'Wendy Williams')


class TestJoin(DatabaseTestCase):
    def test_2_relations_1_common_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        addresses = Table('addresses', metadata,
                          Column('address_id', Integer, primary_key=True),
                          Column('user_id', None, ForeignKey('users.user_id')),
                          Column('email_address', String(50), nullable=False))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])
            conn.execute(addresses.insert(), [
                {'user_id': 1, 'email_address': 'jack@yahoo.com'},
                {'user_id': 1, 'email_address': 'jack@msn.com'},
                {'user_id': 2, 'email_address': 'www@www.org'},
                {'user_id': 2, 'email_address': 'wendy@aol.com'},
            ])

        join_result = db.natural_join(self.engine, [
            {'name': 'users', 'attributes': {'user_id': Integer,
                                             'user_name': String,
                                             'user_fullname': String}},
            {'name': 'addresses', 'attributes': {'address_id': Integer,
                                                 'user_id': Integer,
                                                 'email_address': String}},
        ], {'user_fullname': String, 'email_address': String})

        self.assertEqual(len(join_result), 4)
        self.assertIn({'user_fullname': 'Jack Jones',
                       'email_address': 'jack@yahoo.com'}, join_result)
        self.assertNotIn({'user_fullname': 'Jack Jones',
                          'email_address': 'wendy@aol.com'}, join_result)

    def test_2_relations_2_common_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        students = Table('students', metadata,
                         Column('student_id', Integer, primary_key=True),
                         Column('group_id', Integer, primary_key=True),
                         Column('student_fullname', String(50)))
        marks = Table('marks', metadata,
                      Column('student_id', Integer,
                             ForeignKey('students.student_id'),
                             primary_key=True,
                             autoincrement=False),
                      Column('group_id', Integer, primary_key=True,
                             autoincrement=False),
                      Column('discipline_id', Integer, primary_key=True,
                             autoincrement=False),
                      Column('mark', Integer))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(students.insert(), [
                {'student_id': 1, 'group_id': 803,
                 'student_fullname': 'Jack Jones'},
                {'student_id': 2, 'group_id': 803,
                 'student_fullname': 'Wendy Williams'},
                {'student_id': 3, 'group_id': 804,
                 'student_fullname': 'Chris O Connel'},
                {'student_id': 4, 'group_id': 805,
                 'student_fullname': 'Kyle Crane'},
            ])
            conn.execute(marks.insert(), [
                {'student_id': 1, 'group_id': 803, 'discipline_id': 1,
                 'mark': 5},
                # when the student was in another group
                {'student_id': 1, 'group_id': 804, 'discipline_id': 6,
                 'mark': 3},
                {'student_id': 1, 'group_id': 803, 'discipline_id': 2,
                 'mark': 5},
            ])

        join_result = db.natural_join(self.engine, [
            {'name': 'students', 'attributes': {'student_id': Integer,
                                                'group_id': String,
                                                'student_fullname': String}},
            {'name': 'marks', 'attributes': {'student_id': Integer,
                                             'group_id': Integer,
                                             'discipline_id': Integer,
                                             'mark': Integer}},
        ], {'student_fullname': String, 'mark': String})

        self.assertEqual(len(join_result), 2)
        self.assertIn({'student_fullname': 'Jack Jones',
                       'mark': 5}, join_result)
        self.assertNotIn({'student_fullname': 'Jack Jones',
                          'mark': 3}, join_result)

    def test_2_relations_no_common_attributes(self):
        """
        In case there are no common attributes join should turn into Cartesian
        Product.
        """
        metadata = MetaData(self.engine, reflect=True)
        products = Table('products', metadata,
                         Column('product_id', Integer, primary_key=True),
                         Column('product_name', String(50), nullable=False))
        regions = Table('regions', metadata,
                        Column('region_id', Integer, primary_key=True),
                        Column('region_name', String(50), nullable=False))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(products.insert(), [
                {'product_id': 1, 'product_name': 'Apples'},
                {'product_id': 2, 'product_name': 'Bananas'}
            ])
            conn.execute(regions.insert(), [
                {'region_id': 1, 'region_name': 'Alaska'},
                {'region_id': 2, 'region_name': 'Texas'},
                {'region_id': 3, 'region_name': 'California'},
                {'region_id': 4, 'region_name': 'Florida'}
            ])

        join_result = db.natural_join(self.engine, [
            {'name': 'products', 'attributes': {'product_id': Integer,
                                                'product_name': String}},
            {'name': 'regions', 'attributes': {'region_id': Integer,
                                               'region_name': String}},
        ], {'product_name': String, 'region_name': String})

        self.assertEqual(len(join_result), 8)
        self.assertIn({'product_name': 'Apples',
                       'region_name': 'Alaska'}, join_result)
        self.assertIn({'product_name': 'Bananas',
                       'region_name': 'California'}, join_result)
        self.assertIn({'product_name': 'Bananas',
                       'region_name': 'Florida'}, join_result)

    def test_3_relations_single_path(self):
        """
        In case of 3 relations connected in chain this path should be found and
        corresponding join returned.
        """
        metadata = MetaData(self.engine, reflect=True)
        products = Table('products', metadata,
                         Column('product_id', Integer, primary_key=True),
                         Column('product_name', String(50), nullable=False))
        regions = Table('regions', metadata,
                        Column('region_id', Integer, primary_key=True),
                        Column('region_name', String(50), nullable=False))

        # This is the relation that connects first two not only via common
        # attributes but, more importantly via Multivalued dependency (EMP):
        # NULL -> product_id (region_id)
        product_prices = Table('product_prices', metadata,
                               Column('price_id', Integer, primary_key=True),
                               Column('product_id', Integer,
                                      ForeignKey('products.product_id')),
                               Column('region_id', Integer,
                                      ForeignKey('regions.region_id')),
                               Column('price', Float))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(products.insert(), [
                {'product_id': 1, 'product_name': 'Apples'},
                {'product_id': 2, 'product_name': 'Bananas'}
            ])
            conn.execute(regions.insert(), [
                {'region_id': 1, 'region_name': 'Alaska'},
                {'region_id': 2, 'region_name': 'Texas'},
                {'region_id': 3, 'region_name': 'California'},
                {'region_id': 4, 'region_name': 'Florida'}
            ])
            conn.execute(product_prices.insert(), [
                {'price_id': 1, 'product_id': 1, 'region_id': 1, 'price': 45.32},
                {'price_id': 2, 'product_id': 1, 'region_id': 2, 'price': 12.6},
                {'price_id': 3, 'product_id': 1, 'region_id': 3, 'price': 65.1},
                {'price_id': 4, 'product_id': 2, 'region_id': 1, 'price': 78},
                {'price_id': 5, 'product_id': 2, 'region_id': 4, 'price': 8.55}
            ])

        join_result = db.natural_join(self.engine, [
            {'name': 'product_prices',
             'attributes': {'price_id': Integer, 'product_id': Integer,
                            'region_id': Integer, 'price': Float}},
            {'name': 'products', 'attributes': {'product_id': Integer,
                                                'product_name': String}},
            {'name': 'regions', 'attributes': {'region_id': Integer,
                                               'region_name': String}}
        ], {'product_name': String, 'region_name': String, 'price': Float})

        self.assertEqual(len(join_result), 5)
        self.assertIn({'product_name': 'Apples',
                       'region_name': 'Alaska', 'price': 45.32}, join_result)
        self.assertNotIn({'product_name': 'Bananas',
                          'region_name': 'Texas', 'price': 12.6}, join_result)


class TestGetData(DatabaseTestCase):
    def test_simple(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])

        res = get_data(self.engine, 'users', ['user_name', 'user_fullname'])

        first_row = res[0]
        self.assertEqual(first_row['user_name'], 'jack')
        self.assertEqual(first_row['user_fullname'], 'Jack Jones')
        second_row = res[1]
        self.assertEqual(second_row['user_name'], 'wendy')
        self.assertEqual(second_row['user_fullname'], 'Wendy Williams')

    def test_with_constraint_dict(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])

        res = get_data(self.engine, 'users', ['user_name', 'user_fullname'],
                       constraint={'user_fullname': 'Jack Jones'})

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['user_name'], 'jack')
        self.assertEqual(res[0]['user_fullname'], 'Jack Jones')


class TestToClause(DatabaseTestCase):
    """
    This test suite verifies SQLAlchemy Column clause parsing rather implicitly
    via `get_data` routine
    """
    def prepare_data(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer),
                      Column('user_name', String(20)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_id': None, 'user_name': 'jack'},
                {'user_id': 1, 'user_name': 'wendy'},
                {'user_id': 4, 'user_name': 'brad'},
                {'user_id': 5, 'user_name': 'breck'},
                {'user_id': 6, 'user_name': 'chris'},
            ])

    def test_simple_eq(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': '=', 'value': 4}]]
        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 1)
        self.assertIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': 1}, rows)

    def test_simple_gt(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': '>', 'value': 4}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 5}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': 4}, rows)

    def test_simple_le(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': '<=', 'value': 4}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 1}, rows)
        self.assertIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': 5}, rows)
        self.assertNotIn({'user_id': 6}, rows)

    def test_simple_between(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': 'BETWEEN',
                        'value': [4, 5]}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 4}, rows)
        self.assertIn({'user_id': 5}, rows)
        self.assertNotIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': 6}, rows)

    def test_simple_not_between(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': 'NOT BETWEEN',
                        'value': [4, 5]}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 1}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': 5}, rows)

    def test_simple_null_comparison(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': '=',
                        'value': None}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 1)
        self.assertIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': 5}, rows)
        self.assertNotIn({'user_id': 6}, rows)

        constraint = [[{'attribute': 'user_id', 'operator': '<>',
                        'value': None}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 4)
        self.assertIn({'user_id': 1}, rows)
        self.assertIn({'user_id': 4}, rows)
        self.assertIn({'user_id': 5}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': None}, rows)

    def test_simple_in(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': 'IN',
                        'value': [1, 2, 3, 4]}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 1}, rows)
        self.assertIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 5}, rows)
        self.assertNotIn({'user_id': 6}, rows)

    def test_simple_not_in(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': 'NOT IN',
                        'value': [1, 2, 3, 4]}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 5}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': 4}, rows)

    def test_simple_like(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_name', 'operator': 'LIKE',
                        'value': '%ck'}]]

        rows = db.get_data(self.engine, 'users', ['user_name'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_name': 'jack'}, rows)
        self.assertIn({'user_name': 'breck'}, rows)
        self.assertNotIn({'user_name': 'wendy'}, rows)
        self.assertNotIn({'user_name': 'brad'}, rows)
        self.assertNotIn({'user_name': 'chris'}, rows)

    def test_simple_not_like(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_name', 'operator': 'NOT LIKE',
                        'value': 'b%'}]]

        rows = db.get_data(self.engine, 'users', ['user_name'], constraint)

        self.assertEqual(len(rows), 3)
        self.assertIn({'user_name': 'jack'}, rows)
        self.assertIn({'user_name': 'chris'}, rows)
        self.assertIn({'user_name': 'wendy'}, rows)
        self.assertNotIn({'user_name': 'breck'}, rows)
        self.assertNotIn({'user_name': 'brad'}, rows)

    def test_conjunction_clause(self):
        self.prepare_data()
        constraint = [[{'attribute': 'user_id', 'operator': 'NOT IN',
                        'value': [1, 2, 3, 4]},
                       {'attribute': 'user_name', 'operator': 'LIKE',
                        'value': '%r%'}]]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 2)
        self.assertIn({'user_id': 5}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': 4}, rows)

    def test_disjunctive_normal_form(self):
        self.prepare_data()
        constraint = [
            [{'attribute': 'user_id', 'operator': 'NOT IN',
              'value': [1, 2, 3, 4]},
             {'attribute': 'user_name', 'operator': 'LIKE', 'value': '%r%'}],
            [{'attribute': 'user_id', 'operator': '<', 'value': 3},
             {'attribute': 'user_name', 'operator': '=', 'value': 'wendy'}],
        ]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 3)
        self.assertIn({'user_id': 1}, rows)
        self.assertIn({'user_id': 5}, rows)
        self.assertIn({'user_id': 6}, rows)
        self.assertNotIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 4}, rows)

    def test_empty_conjunction_clause(self):
        self.prepare_data()
        constraint = [
            [],
            [{'attribute': 'user_id', 'operator': '<', 'value': 3},
             {'attribute': 'user_name', 'operator': '=', 'value': 'wendy'}],
        ]

        rows = db.get_data(self.engine, 'users', ['user_id'], constraint)

        self.assertEqual(len(rows), 1)
        self.assertIn({'user_id': 1}, rows)
        self.assertNotIn({'user_id': None}, rows)
        self.assertNotIn({'user_id': 4}, rows)
        self.assertNotIn({'user_id': 5}, rows)
        self.assertNotIn({'user_id': 6}, rows)


class TestGetRows(DatabaseTestCase):
    def test(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
            ])

        res = get_rows(self.engine, {'name': users.name,
                                     'attributes': users.c._data})

        first_row = res[0]
        self.assertEqual(first_row['user_name'], 'jack')
        self.assertEqual(first_row['user_fullname'], 'Jack Jones')
        second_row = res[1]
        self.assertEqual(second_row['user_name'], 'wendy')
        self.assertEqual(second_row['user_fullname'], 'Wendy Williams')


class TestDeleteRows(DatabaseTestCase):
    def test_successful(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        rows = [
            {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
            {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'},
            {'user_name': 'chris', 'user_fullname': 'Chris Kyle'},
        ]
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), rows)

        delete_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    rows[:2])

        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 1)
        self.assertEqual(all_records[0]['user_name'], rows[2]['user_name'])
        self.assertEqual(all_records[0]['user_fullname'],
                         rows[2]['user_fullname'])

    def test_empty(self):
        """
        No rows should be removed if `rows` is empty iterator
        """
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        rows = [{'user_name': 'jack', 'user_fullname': 'Jack Jones'}]
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), rows)

        # case 1: test with empty list
        delete_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    [])

        # check that row is still there
        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 1)

        # case 2: test with empty generator
        def rows_gen():
            if 1 < 0:
                yield 1
        delete_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    rows_gen())

        # check that row is still there
        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 1)

    def test_non_existing(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        rows = [{'user_name': 'jack', 'user_fullname': 'Jack Jones'}]
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), rows)

        delete_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    [{'user_name': 'non existing',
                      'user_fullname': 'also does not exist'}])

        # check that row is still there
        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 1)
        self.assertEqual(all_records[0]['user_name'], rows[0]['user_name'])
        self.assertEqual(all_records[0]['user_fullname'],
                         rows[0]['user_fullname'])


class TestDeleteUnsatisfied(DatabaseTestCase):
    def test(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        jack = {'user_name': 'jack', 'user_fullname': 'Jack Jones'}
        wendy = {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'}
        chris = {'user_name': 'chris', 'user_fullname': 'Chris Kyle'}
        rows = [jack, wendy, chris]
        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), rows)

        constraint = [[{'attribute': 'user_name', 'operator': 'LIKE',
                        'value': 'j%'}],
                      [{'attribute': 'user_name', 'operator': 'LIKE',
                        'value': '%r%'}]]

        delete_unsatisfied(self.engine, {'name': 'users'},
                           constraint=constraint)

        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 2)
        self.assertEqual(all_records[0]['user_name'], jack['user_name'])
        self.assertEqual(all_records[1]['user_name'], chris['user_name'])


class TestInsertRows(DatabaseTestCase):
    def test_empty(self):
        """
        Calling with empty list should result in no rows being inserted
        :return:
        """
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        rows = []

        insert_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    rows)

        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 0)

        # case 2: generator expression
        def rows_gen():
            if 1 < 0:
                yield 1

        insert_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    rows_gen())

        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 0)

    def test_successful(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()
        rows = [
            {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
            {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'},
            {'user_name': 'chris', 'user_fullname': 'Chris Kyle'},
        ]

        insert_rows(self.engine, {'name': users.name,
                                  'attributes': users.c._data},
                    rows)

        with self.engine.connect() as conn:
            res = conn.execute(users.select())
            all_records = res.fetchall()
        self.assertEqual(len(all_records), 3)
        usernames = [r['user_name'] for r in all_records]
        for row in rows:
            self.assertIn(row['user_name'], usernames)


class TestCountAttributes(DatabaseTestCase):
    def test_single_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack19', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack1990', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack113', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Christoper'},
                {'user_name': 'wendy<3', 'user_fullname': 'Wendy Williams'},
            ])

        counts = db.count_attributes(self.engine, 'users', ['user_fullname'])

        self.assertEqual(len(counts), 1)
        self.assertEqual(counts[0], 3)

    def test_single_attribute_dict_form(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack19', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack1990', 'user_fullname': 'Jack Jones'},
                {'user_name': 'jack113', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Williams'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Christoper'},
                {'user_name': 'wendy<3', 'user_fullname': 'Wendy Williams'},
            ])

        counts = db.count_attributes(self.engine, 'users',
                                     {'user_fullname': 'STRING'})

        self.assertEqual(len(counts), 1)
        self.assertEqual(counts[0], 3)

    def test_several_attributes(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)),
                      Column('user_gender', String(1)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones',
                 'user_gender': 'm'},
                {'user_name': 'jack19', 'user_fullname': 'Jack Jones',
                 'user_gender': 'm'},
                {'user_name': 'jack1990', 'user_fullname': 'Jack Jones',
                 'user_gender': None},
                {'user_name': 'jack113', 'user_fullname': 'Jack Jones',
                 'user_gender': 'm'},
                {'user_name': 'wendy', 'user_fullname': 'Wendy Adams',
                 'user_gender': 'f'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Christoper',
                 'user_gender': 'f'},
                {'user_name': 'wendy<3', 'user_fullname': 'Wendy Williams',
                 'user_gender': 'f'},
            ])

        counts = db.count_attributes(self.engine, 'users',
                                     ['user_fullname', 'user_gender'])

        self.assertEqual(len(counts), 2)
        self.assertEqual(counts[0], 4)
        self.assertEqual(counts[1], 2)  # `None` isn't counted

        # reverse the order
        counts = db.count_attributes(self.engine, 'users',
                                     ['user_gender', 'user_fullname'])

        self.assertEqual(len(counts), 2)
        self.assertEqual(counts[0], 2)  # `None` isn't counted
        self.assertEqual(counts[1], 4)

    def test_missing_attributes(self):
        metadata = MetaData(self.engine, reflect=True)
        Table('users', metadata,
              Column('user_id', Integer, primary_key=True),
              Column('user_name', String(20)),
              Column('user_fullname', String(50)),
              Column('user_gender', String(1)))
        metadata.create_all()

        self.assertRaises(InternalError, db.count_attributes,
                          self.engine, 'users',
                          ['this attribute is not there'])


class TestProject(DatabaseTestCase):
    def test_single_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy<3', 'user_fullname': 'Wendy Williams'},
                {'user_name': 'jack19', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Christoper'},
            ])

        projection = db.project(self.engine, 'users', ['user_name'])

        self.assertEqual(len(projection), 4)
        self.assertEqual(projection[0], {'user_name': 'jack'})
        self.assertEqual(projection[1], {'user_name': 'jack19'})
        self.assertEqual(projection[2], {'user_name': 'wendy3'})
        self.assertEqual(projection[3], {'user_name': 'wendy<3'})

        projection = db.project(self.engine, 'users', ['user_fullname'])

        self.assertEqual(len(projection), 4)
        self.assertEqual(projection[0], {'user_fullname': 'Jack Jones'})
        self.assertEqual(projection[1], {'user_fullname': 'Jack Jones'})
        self.assertEqual(projection[2], {'user_fullname': 'Wendy Christoper'})
        self.assertEqual(projection[3], {'user_fullname': 'Wendy Williams'})

    def test_multiple_attributes(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': 'jack19', 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Williams'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Christoper'},
                {'user_name': 'jack19', 'user_fullname': 'Jack Bones'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Cruz'},
            ])

        projection = db.project(self.engine, 'users', ['user_fullname',
                                                       'user_name'])

        self.assertEqual(len(projection), 5)
        self.assertEqual(projection[0], {'user_name': 'jack19',
                                         'user_fullname': 'Jack Bones'})
        self.assertEqual(projection[1], {'user_name': 'jack19',
                                         'user_fullname': 'Jack Jones'})
        self.assertEqual(projection[2], {'user_fullname': 'Wendy Christoper',
                                         'user_name': 'wendy3'})
        self.assertEqual(projection[3], {'user_name': 'wendy3',
                                         'user_fullname': 'Wendy Cruz'})
        self.assertEqual(projection[4], {'user_fullname': 'Wendy Williams',
                                         'user_name': 'wendy3'})

    def test_with_nulls(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('user_id', Integer, primary_key=True),
                      Column('user_name', String(20)),
                      Column('user_fullname', String(50)))
        metadata.create_all()

        # populate with data
        with self.engine.connect() as conn:
            conn.execute(users.insert(), [
                {'user_name': None, 'user_fullname': 'Jack Jones'},
                {'user_name': 'wendy3', 'user_fullname': 'Wendy Williams'},
                {'user_name': None, 'user_fullname': 'Wendy Williams'},
                {'user_name': 'wendy3', 'user_fullname': None},
            ])

        projection = db.project(self.engine, 'users', ['user_name',
                                                       'user_fullname'])

        self.assertEqual(len(projection), 4)
        self.assertEqual(projection[0], {'user_name': None,
                                         'user_fullname': 'Jack Jones'})
        self.assertEqual(projection[1], {'user_name': None,
                                         'user_fullname': 'Wendy Williams'})
        self.assertEqual(projection[2], {'user_name': 'wendy3',
                                         'user_fullname': None})
        self.assertEqual(projection[3], {'user_name': 'wendy3',
                                         'user_fullname': 'Wendy Williams'})
