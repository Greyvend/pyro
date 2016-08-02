from sqlalchemy import MetaData, Integer, String, Column, Table, ForeignKey
from sqlalchemy.sql.sqltypes import _Binary, Text, LargeBinary, Float
from sqlalchemy.dialects.mysql import REAL

from pyro import db
from pyro.db import create_table, _transform_column_type, _execute, \
    get_rows
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


class Test_execute(DatabaseTestCase):
    """
    Weird bug: test hangs with Python 3 if the class name contains capital 'E'.
    """
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
