"""
Some data types can't be automatically translated so we explicitly define
transformation strategy.
"""
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.dialects import mysql


@compiles(mysql.YEAR, 'sqlite')
def compile_sqlite_tinyint(type_, compiler, **kw):
    return 'INTEGER'
