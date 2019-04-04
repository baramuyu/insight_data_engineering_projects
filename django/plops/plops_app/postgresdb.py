import psycopg2

#     - *dbname*: the database name
#     - *database*: the database name (only as keyword argument)
#     - *user*: user name used to authenticate
#     - *password*: password used to authenticate
#     - *host*: database host address (defaults to UNIX socket if not provided)
#     - *port*: connection port number (defaults to 5432 if not provided)

class PostgresAdapter(object):
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def execute(self, sql):
        conn = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password, host=self.host)
        cur = conn.cursor()
        cur.execute(sql)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
