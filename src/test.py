import server
from server import r

db = server.create_db()

if __name__ == '__main__':
    changefeed = r.table("event_data").get("rankings").changes(include_initial=True).run(db)
    for change in changefeed:
        print(change)