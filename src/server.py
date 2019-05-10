from dynaconf import settings
import rethinkdb
import timeloop

r = rethinkdb.RethinkDB()  # Global rethink instance

# Utilities for creating connections, etc.


def create_db():
    address = settings["rethinkdb_address"]
    port = settings["rethinkdb_port"]
    db = settings["rethinkdb_database"]
    conn = r.connect(address, port)
    conn.use(db)
    return conn


def create_scheduler():
    tl = timeloop.Timeloop()
    return tl


def setup():
    return create_scheduler(), create_db()


# Various utility functions


def init_table(name, db):
    try:
        r.table_create(name).run(db)
    except r.ReqlOpFailedError:
        pass  # Ignore errors here, this just means the table already doesn't exist
