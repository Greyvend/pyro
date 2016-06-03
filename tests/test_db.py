from sqlalchemy import MetaData, Integer, String
from sqlalchemy.sql.sqltypes import _Binary, Text, LargeBinary
from sqlalchemy.dialects.mysql import REAL
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
