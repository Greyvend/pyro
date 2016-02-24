import json
from copy import deepcopy

from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL


config = {}


def column_name(column):
    return column.name


if __name__ == '__main__':
    with open('config.json') as config_file:
        config = json.load(config_file)
        # read RDB schema: all tables info: their attributes and dependencies
        relations = []
        dependencies = deepcopy(config.get('multi valued dependencies')) or []

        engine = create_engine(URL(**config['database']))
        metadata = MetaData(engine, reflect=True)

        # fill relations list
        relations = metadata.tables.keys()

        # fill dependencies from Primary keys and Unique constraints
        for table, table_data in metadata.tables.iteritems():
            if table == 'address':
                print 'I am here'
            pk = set(map(column_name, table_data.primary_key.columns))
            all_columns = set(map(column_name, table_data.columns))
            primary_key_dep = {
                'left': pk,
                'right': all_columns - pk
            }
            dependencies.append(primary_key_dep)

            unique_indexes = filter(lambda i: i.unique, table_data.indexes)
            for i in unique_indexes:
                key = set(map(column_name, i.columns))
                unique_dep = {
                    'left': key,
                    'right': all_columns - key
                }
                dependencies.append(unique_dep)
