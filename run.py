import json

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from pyro import db, tj, transformation
from pyro.utils import relation_name

# TODO: switch to python 3.5
if __name__ == '__main__':
    with open('config.json') as config_file:
        config = json.load(config_file)

    # connect to source Database
    source_engine = create_engine(URL(**config['source_db']))

    relations, dependencies = db.get_schema(source_engine)

    # get multi valued dependencies from the config
    mvd = config.get('multi valued dependencies', [])
    # transform lists of attributes to sets of attributes
    mvd = [{part: set(attributes) for part, attributes in dep.iteritems()}
           for dep in mvd]
    dependencies.extend(mvd)

    # get measure
    measure = config['measure']

    # Start transformation
    # build contexts
    contexts = []
    base_total = {relation_name(measure)}
    # build dimension contexts
    for dimension in config['dimensions']:
        # extract relation names from extended attribute names in config
        base = set(map(relation_name, dimension['attributes']))
        base_total |= base
        # for now pick first found context
        context = transformation.contexts(relations, base, dependencies).next()
        contexts.append(context)

    # build application context
    try:
        app_context = transformation.contexts(relations, base_total,
                                              dependencies).next()
    except StopIteration:
        # no combination satisfies Lossless Join property. Pick all relations
        app_context = relations
    contexts.append(app_context)

    # connect to the output DB
    cube_engine = create_engine(URL(**config['cube_db']))

    for context in contexts:
        tj.build(context, dependencies, source_engine, cube_engine)
