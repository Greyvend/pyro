__author__ = 'mosin'
import pyodbc

print pyodbc.__file__

cndBase = pyodbc.connect("DRIVER={MySQL ODBC 3.51 Driver}; SERVER=localhost;"
                         "DATABASE=test; UID=root; PASSWORD=mysql;OPTION=3;")
