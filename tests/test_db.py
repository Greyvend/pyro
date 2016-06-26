from sqlalchemy import MetaData, Integer, String, Column, Table, ForeignKey
from sqlalchemy.sql.sqltypes import _Binary, Text, LargeBinary
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
    def test_simple_one_common_attribute(self):
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

        sql_join = db.join(self.engine, [
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
