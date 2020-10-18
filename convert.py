import sqlite3
import json
 
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d
 
connection = sqlite3.connect("twitter.sqlite")
connection.row_factory = dict_factory
 
cursor = connection.cursor()
 
cursor.execute("SELECT * FROM LINKS")
 
# fetch all or one we'll go for all.
 
links = cursor.fetchall()
cursor.execute("SELECT * FROM USERS")
users = cursor.fetchall()
json_format = { 'links': links, 'users': users }

with open('data.json', 'w') as outfile:
    json.dump(json_format, outfile)
 
connection.close()