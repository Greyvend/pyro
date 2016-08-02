import json
from unittest import TestCase
import logging
from os import path

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import database_exists, create_database, drop_database


class DatabaseTestCase(TestCase):
    """TestCase class that clears DB between the tests"""

    def __init__(self, *args, **kwargs):
        config_path = path.abspath(path.join(path.dirname(__file__), '..'))
        with open(path.join(config_path, 'config.json')) as config_file:
            config = json.load(config_file)
        self.engine = create_engine(URL(**config['test_db']))
        self.config = config
        super(DatabaseTestCase, self).__init__(*args, **kwargs)

    def setUp(self):
        # disable logging when tests are running
        logging.disable(logging.CRITICAL)
        if database_exists(self.engine.url):
            drop_database(self.engine.url)
        create_database(self.engine.url)

    def tearDown(self):
        if database_exists(self.engine.url):
            drop_database(self.engine.url)
        # enable logging back
        logging.disable(logging.NOTSET)
