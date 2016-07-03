from sqlalchemy import MetaData, Integer, String, Column, Table, ForeignKey
from sqlalchemy.sql.sqltypes import _Binary, Text, LargeBinary, Float
from sqlalchemy.dialects.mysql import REAL

from pyro import db
from pyro.db import create_table, transform_column_type
from tests.alchemy import DatabaseTestCase


class TestCreateTable(DatabaseTestCase):
    def test_new_table(self):
        metadata = MetaData(self.engine, reflect=True)
        name = 'test_table'
        attributes = {'first': Integer, 'second': String(10)}

        self.assertEqual(len(metadata.tables), 0)

        create_table(name, attributes, self.engine)
        metadata.reflect()
        self.assertEqual(len(metadata.tables), 1)
        self.assertEqual(len(metadata.tables['test_table'].columns), 2)

    def test_idempotence(self):
        metadata = MetaData(self.engine, reflect=True)
        name = 'test_table'
        attributes = {'first': Integer, 'second': String(10)}

        self.assertEqual(len(metadata.tables), 0)

        create_table(name, attributes, self.engine)
        create_table(name, attributes, self.engine)

        metadata.reflect()
        self.assertEqual(len(metadata.tables), 1)


class TestTransformColumnType(DatabaseTestCase):
    def test_handled_types(self):
        integer = Integer()
        str = String(15)
        binary = _Binary()
        self.assertEqual(transform_column_type(integer), Integer)
        self.assertEqual(transform_column_type(str), Text)
        self.assertEqual(transform_column_type(binary), LargeBinary)

    def test_unhandled_type(self):
        real = REAL()

        transformed_type = transform_column_type(real)
        self.assertIsNone(transformed_type.collation)
        self.assertEqual(transformed_type.encoding, 'utf-8')


class TestJoin(DatabaseTestCase):
    def test_2_relations_1_common_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(20)),
                      Column('fullname', String(50)))
        addresses = Table('addresses', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('user_id', None, ForeignKey('users.id')),
                          Column('email_address', String(50), nullable=False))
        metadata.create_all()

        # populate with data
        conn = self.engine.connect()
        conn.execute(users.insert(), [
            {'name': 'jack', 'fullname': 'Jack Jones'},
            {'name': 'wendy', 'fullname': 'Wendy Williams'}
        ])
        conn.execute(addresses.insert(), [
            {'user_id': 1, 'email_address': 'jack@yahoo.com'},
            {'user_id': 1, 'email_address': 'jack@msn.com'},
            {'user_id': 2, 'email_address': 'www@www.org'},
            {'user_id': 2, 'email_address': 'wendy@aol.com'},
        ])

        sql_join = db.natural_join(self.engine, [
            {'name': 'users', 'attributes': {'id': Integer, 'name': String,
                                             'fullname': String}},
            {'name': 'addresses', 'attributes': {'id': Integer,
                                                 'user_id': Integer,
                                                 'email_address': String}},
        ], [{'fullname': String}, {'email_address': String}])
        join_result = sql_join.fetchall()
        self.assertEqual(len(join_result), 4)
        self.assertIn(('Jack Jones', 'jack@yahoo.com'), join_result)
        self.assertNotIn(('Jack Jones', 'wendy@aol.com'), join_result)

    def test_2_relations_2_common_attribute(self):
        metadata = MetaData(self.engine, reflect=True)
        users = Table('users', metadata,
                      Column('id', Integer, primary_key=True),
                      Column('name', String(20)),
                      Column('fullname', String(50)))
        addresses = Table('addresses', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('name', String(20)),
                          Column('user_id', None, ForeignKey('users.id')))
        metadata.create_all()

        # populate with data
        conn = self.engine.connect()
        conn.execute(users.insert(), [
            {'name': 'jack', 'fullname': 'Jack Jones'},
            {'name': 'wendy', 'fullname': 'Wendy Williams'}
        ])
        conn.execute(addresses.insert(), [
            {'user_id': 1, 'name': 'Local Address'},
            {'user_id': 1, 'name': 'Local Address 2'},
            {'user_id': 2, 'name': 'Local Address 3'},
            {'user_id': 2, 'name': 'Local Address 4'},
        ])

        sql_join = db.natural_join(self.engine, [
            {'name': 'users', 'attributes': {'id': Integer, 'name': String,
                                             'fullname': String}},
            {'name': 'addresses', 'attributes': {'id': Integer,
                                                 'user_id': Integer,
                                                 'name': String}},
        ], [{'fullname': String}, {'name': String}])
        join_result = sql_join.fetchall()
        self.assertEqual(len(join_result), 4)
        # !Because we don't specify extended attribute names here the 'name'
        # attribute used in JOIN will be the one from 'users' table (first
        # occurred).
        self.assertIn(('Jack Jones', 'jack'), join_result)
        self.assertNotIn(('Jack Jones', 'wendy'), join_result)

    def test_2_relations_no_common_attributes(self):
        """
        In case there are no common attributes join should turn into Cartesian
        Product.
        """
        metadata = MetaData(self.engine, reflect=True)
        products = Table('products', metadata,
                         Column('id', Integer, primary_key=True),
                         Column('name', String(50), nullable=False))
        regions = Table('regions', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('region_name', String(50), nullable=False))

        # This is the relation that connects first two not only via common
        # attributes but, more importantly via Multivalued dependency (EMP):
        # NULL -> product_id (region_id)
        product_prices = Table('product_prices', metadata,
                               Column('product_id', Integer,
                                      ForeignKey('products.id'),
                                      primary_key=True),
                               Column('region_id', Integer,
                                      ForeignKey('regions.id')),
                               Column('price', Float))
        metadata.create_all()

        # populate with data
        conn = self.engine.connect()
        conn.execute(products.insert(), [
            {'id': 1, 'name': 'Apples'},
            {'id': 2, 'name': 'Bananas'}
        ])
        conn.execute(regions.insert(), [
            {'id': 1, 'region_name': 'Alaska'},
            {'id': 2, 'region_name': 'Texas'},
            {'id': 3, 'region_name': 'California'},
            {'id': 4, 'region_name': 'Florida'}
        ])

        sql_join = db.natural_join(self.engine, [
            {'name': 'products', 'attributes': {'id': Integer,
                                                'name': String}},
            {'name': 'regions', 'attributes': {'id': Integer, 'name': String}},
        ], [{'name': String}, {'region_name': String}])
        join_result = sql_join.fetchall()
        self.assertEqual(len(join_result), 4)
        self.assertIn(('Jack Jones', 'jack@yahoo.com'), join_result)
        self.assertNotIn(('Jack Jones', 'wendy@aol.com'), join_result)
