__author__ = 'mosin'
from collections import namedtuple

import queries


# FunctionalDependency = namedtuple(typename="FunctionalDependency",
#                                   field_names=["Left", "Right"])

Attribute = namedtuple(typename="Attribute",
                       field_names=["name", "relation_name", "value_amount"])
"""
relation_name.name - full name of an attribute (ex. Worker.First_Name)
value_amount - amount of different values this attribute takes
#valueType - dbms defined value of attribute's datatype (ex. INT, VARCHAR etc)
"""

Relation = namedtuple(typename="Relation",
                      field_names=["name", "attributes", "primary_key"])
"""
attributes - tuple of attributes (ex. (number, firstName, lastName, salary))
primary_key - same, but only primary ones
"""

Schema = namedtuple(typename="Schema",
                    field_names=["relations", "functional_dependencies",
                                 "multi_valued_dependencies"])
"""
relations - list of relations
functional_dependencies - set of FDs of database schema
multi_valued_dependencies - set of MVDs of database schema
"""


class Rdb:
    def __init__(self, dbms, host, login, password, db):
        self._db_connection = {
            "mysql": queries.Mysql(host, login, password, db),
            "oracle": queries.Dbms()}[dbms]
        self._schema = Schema(relations=[],
                              functional_dependencies=set(),
                              multi_valued_dependencies=set())
        self._pull_schema()

    def _pull_schema(self):
        #1. Get all tables of database
        relation_names = self._db_connection.tables() # returns tuple with
                                                    # relation names
        #2. Fetch their attributes and PK deps
        for r_name in relation_names:
            #attr_amounts = self._db_connection.count_attributes(r_name, attributes)
            # attr_list = []
            # for i, aName in enumerate(attributes):
            #     # attr = Attribute(name=aName,
            #     #                  relation_name=r_name,
            #     #                  value_amount=attr_amounts(i))
            #     attr_list.append(attr)
            attrs = self._db_connection.columns(r_name)
            pk = self._db_connection.primary_key(r_name)
            relation = Relation(name=r_name,
                                attributes=attrs,
                                primary_key=pk)
            #3. Fill Schema
            if pk:
                #fds = [(pk, attr) for attr in attrs if attr not in pk]
                for attr in attrs:
                    if attr not in pk:
                        self._schema.functional_dependencies.add((pk, attr))
            self._schema.relations.append(relation)

    def create_tj(self, dbname, name, relations, attrs, formula):
        pass

#TODO: add interface to complete database usage needs