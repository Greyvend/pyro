#from _mysql_exceptions import OperationalError

__author__ = 'mosin'


class Relation:
    """Abstraction for relational table information"""


class Dbms:
    """general class for database management systems"""

    def __init__(self):
        self._name = "default"
        self._db = None

    def table(self, name):
        return {"name":_getName(name), "attributes":_getAttributes(name)}

    #Queries
    def count_attributes(self, table, attributes):
        """
        Count distinct values of all attributes in relation.
        table - String, name of table to search
        attributes - list of attributes
        """
        query = "select {0} from {1}"
        attrs = ", ".join("count(distinct {0})".format(a) for a in
                          attributes)
        cur = self._db.cursor()
        cur.execute(query.format(attrs, table))
        rec = cur.fetchone()
        return rec

    def inclusion_dependency(self, r1, r2, x):
        """check if R1[X] is a subset of R2[X]"""
        query = """SELECT COUNT(DISTINCT {0}) AS amount
                 FROM {1} JOIN {2}
                 USING ({0})
                 HAVING amount = (SELECT COUNT(DISTINCT {0}) FROM {1})"""
        attrs = ", ".join(x)
        cur = self._db.cursor()
        cur.execute(query.format(attrs, r1, r2))
        rec = cur.fetchone()
        if not rec:
            return False
        else:
            return True

    def select_where(self, table, x, values):
        """
        Return R[X] where 'values' specify particular attribute values.
        If several results are returned, then only the 1st row is evaluated
        """
        query = """SELECT DISTINCT {0}
                 FROM {1}
                 WHERE {2}"""
        attrs = ", ".join(x)
        where = " and ".join("{0} = %s".format(attr) for attr in x)
        cur = self._db.cursor()
        cur.execute(query.format(attrs, table, where), values)
        rec = cur.fetchone()
        return rec

    def primary_key(self, table):
        """
        get all Primary Key attributes
        (several in the case of complex key)
        return tuple, containing all of them
        """
        pass

    def tables(self):
        """
        return tuple, containing  table names of all tables in current db
        """
        pass

    def columns(self, table):
        """
        return tuple, containing  column names of all columns in current table
        """
        pass

    def create_table(self, name, attributes, primary, temp=True):
        """
        name is a string, presenting name of newly created table,
        attributes is a dictionary of name:type elements of table columns,
        primary is a tuple of primary key attributes. primary is a subset of
        attributes keys
        """
        pass

    def drop_table(self, name):
        """
        name is a name of table (in current database by default) to be dropped
        """
        pass


class Mysql(Dbms):

    OperationalError = None

    def __init__(self, host, login, password, db):
        import MySQLdb
        from _mysql_exceptions import OperationalError
        self._db = MySQLdb.connect(host=host, user=login,
                                   passwd=password, db=db)
        self._name = db
        Mysql.OperationalError = MySQLdb.OperationalError

    #Queries

    def primary_key(self, table):
        query = """SELECT COLUMN_NAME
                 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s
                 AND TABLE_NAME = %s
                 AND COLUMN_KEY = %s"""
        cur = self._db.cursor()
        cur.execute(query, (self._name, table, "PRI"))
        records = cur.fetchall()
        res = []
        for rec in records:
            res.append(rec[0])
        return tuple(res)

    def tables(self):
        query = """SELECT TABLE_NAME
                 FROM information_schema.TABLES
                 WHERE TABLE_SCHEMA = %s"""
        cur = self._db.cursor()
        cur.execute(query, (self._name,))
        records = cur.fetchall()
        res = []
        for rec in records:
            res.append(rec[0])
        return tuple(res)

    def columns(self, table):
        query = """SELECT COLUMN_NAME
                 FROM information_schema.COLUMNS
                 WHERE TABLE_SCHEMA = %s
                 AND TABLE_NAME = %s"""
        cur = self._db.cursor()
        cur.execute(query, (self._name, table))
        records = cur.fetchall()
        res = []
        for rec in records:
            res.append(rec[0])
        return tuple(res)

    def create_table(self, name, attributes, primary, temp=True):
        query = """CREATE {0} TABLE {1}(
                 {2},
                 primary key ({3})
                 ) ENGINE=myisam DEFAULT CHARSET=utf8"""
        cur = self._db.cursor()
        if temp:
            tmp = "temporary"
        else:
            tmp = ""
        attrs = ", ".join("{0} {1}".format(attr, type) for attr, type in
                          attributes.iteritems())
        pk = ", ".join(primary)
        print query.format(tmp, name, attrs, pk)
        cur.execute(query.format(tmp, name, attrs, pk))

    def drop_table(self, name):
        query = """DROP TABLE {0}"""
        cur = self._db.cursor()
        try:
            cur.execute(query.format(name))
        except Mysql.OperationalError as e:
            if e.args[0] == 1051:
                pass

    def __del__(self):
        self._db.close()