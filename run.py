import json
import logging

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from pyro import db, tj, transformation
from pyro.utils import relation_name


logging.basicConfig(level=logging.INFO)


if __name__ == '__main__':
    with open('config.json') as config_file:
        config = json.load(config_file)

    # connect to source Database
    logging.info('Connecting to the source DB: {}'.format(
        config['source_db']['database']))
    source_engine = create_engine(URL(**config['source_db']))

    relations, dependencies = db.get_schema(source_engine)

    # get multi valued dependencies from the config
    mvd = config.get('multi valued dependencies', [])
    # transform lists of attributes to sets of attributes
    mvd = [{part: set(attributes) for part, attributes in dep.items()}
           for dep in mvd]
    dependencies.extend(mvd)

    # get measure
    measure = config['measure']

    # Start transformation
    # build contexts
    logging.info('Building dimension contexts...')
    contexts = []
    base_total = {relation_name(measure)}
    # build dimension contexts
    for dimension in config['dimensions']:
        logging.info('Building context for dimension "{}"'.format(
            dimension['name']))
        # extract relation names from extended attribute names in config
        base = set(map(relation_name, dimension['attributes']))
        base_total |= base
        # for now pick first found context
        context = next(transformation.contexts(relations, base, dependencies))
        contexts.append(context)

    # build application context
    logging.info('Building application context')
    try:
        app_context = next(transformation.contexts(relations, base_total,
                                                   dependencies))
    except StopIteration:
        # no combination satisfies Lossless Join property. Pick all relations
        logging.warning('No lossless combinations were found, picking whole '
                        'relation set for application application context')
        app_context = relations
    contexts.append(app_context)

    # connect to the output DB
    logging.info('Connecting to the cube DB: {}'.format(
        config['cube_db']['database']))
    cube_engine = create_engine(URL(**config['cube_db']))

    for context in contexts:
        logging.info('Building Table of Joins for the context {}'.format(
            context))
        tj.build(context, dependencies, source_engine, cube_engine)

    logging.info('The source Database has been successfully transformed to '
                 'OLAP representation!')
