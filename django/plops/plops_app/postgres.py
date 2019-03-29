import psycopg2
import os

#     - *dbname*: the database name
#     - *database*: the database name (only as keyword argument)
#     - *user*: user name used to authenticate
#     - *password*: password used to authenticate
#     - *host*: database host address (defaults to UNIX socket if not provided)
#     - *port*: connection port number (defaults to 5432 if not provided)

def execute(sql):
    conn = connect('occupancy', 'web_server', os.environ['POSTGRES_PASS'], 'ec2-52-39-242-144.us-west-2.compute.amazonaws.com') 
    cur = conn.cursor()
    cur.execute(sql)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def connect(dbname, user, password, host):
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host)