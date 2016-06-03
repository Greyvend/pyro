from sqlalchemy import MetaData, Integer, String

from pyro.db import create_table
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
