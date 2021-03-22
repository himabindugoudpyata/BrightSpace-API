import pyodbc
import pandas
import json

#  Open JSON File
with open('E:\\GIT\\HigherEdAPI\\BrightSpace\\credentials.json') as f:
  credentials = json.load(f)

sql_connection = pyodbc.connect(  'DRIVER={' + credentials['dbdriver'] + '}' +
                                  ';SERVER=' + credentials['dbserver'] +
                                  ';DATABASE=' + credentials['dbdatabase'] +
                                  ';Trusted_Connection=yes;'
                                  )
cursor = sql_connection.cursor()

cursor.execute('''

               CREATE TABLE TEST
               (
               Name nvarchar(50),
               Age int,
               City nvarchar(50)
               )

               ''')

sql_connection.commit()