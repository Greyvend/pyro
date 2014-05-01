__author__ = 'mosin'
import _connection
import core.rdb

muscle = _connection.Mysql("localhost", "root", "mysql", "test")

#----------------------------------#
counter = muscle.count_attributes(table="employee",
                                  attributes=("Name", "Dept"))
print "Overall: " + str(counter)

#----------------------------------#
included = muscle.inclusion_dependency(r1="table1", r2="table2", x=("value",))
if included:
    print "inclusion dependency indeed"
else:
    print "No inclusion"

#----------------------------------#
abValues = muscle.select_where(table="R1", x=("A", "B"), values=("a1", "b2"))

print abValues

#----------------------------------#
muscle.drop_table(name="robson")

#----------------------------------#
muscle.drop_table(name="testing_script")
muscle.create_table(name="testing_script",
                    attributes={"A": "INT", "B": "VARCHAR(20)", "C": "VARCHAR(30)"},
                    primary=("A", "B"), temp=False)

pk = muscle.primary_key(table="testing_script")
print pk

muscle.drop_table(name="testing_script")

#-------------------test receiving table names---------------#
tableNames = muscle.tables()
print tableNames

#-------------------test receiving table names---------------#
employeeColumnNames = muscle.columns(table="employee")
print employeeColumnNames

#-------------------test Rdb class---------------#
muscle.drop_table(name="test_Rdb")
muscle.create_table(name="test_Rdb",
                    attributes={"A": "INT", "B": "VARCHAR(20)", "C": "VARCHAR(30)"},
                    primary=("A", "B"), temp=False)
database = core.rdb.Rdb(dbms="mysql",
                           host="localhost",
                           login="root",
                           password="mysql",
                           db="test")
#for r in database._schema.relations print
for fd in database._schema.functional_dependencies:
    print fd
    #print "left = " + str(fd[0])
    #print "right = " + str(fd[1])
del muscle


