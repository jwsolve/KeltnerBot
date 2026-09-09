import sqlite3
from pathlib import Path

class DB:
    def __init__(self, path):
        self.path = str(path)
        self.init()

    def conn(self):
        c = sqlite3.connect(self.path)
        c.row_factory = sqlite3.Row
        return c

    def init(self):
        with self.conn() as c:
            c.executescript('''
            CREATE TABLE IF NOT EXISTS audit(id INTEGER PRIMARY KEY, ts TEXT, event TEXT, payload TEXT);
            CREATE TABLE IF NOT EXISTS fills(id INTEGER PRIMARY KEY, ts TEXT, symbol TEXT, side TEXT, order_id TEXT, amount REAL, price REAL, fee REAL, raw TEXT);
            CREATE TABLE IF NOT EXISTS backtests(id INTEGER PRIMARY KEY, ts TEXT, symbol TEXT, timeframe TEXT, params TEXT, result TEXT);
            ''')

    def audit(self, ts, event, payload):
        import json
        with self.conn() as c:
            c.execute('INSERT INTO audit(ts,event,payload) VALUES(?,?,?)', (ts,event,json.dumps(payload, default=str)))

    def fill(self, row):
        import json
        with self.conn() as c:
            c.execute('INSERT INTO fills(ts,symbol,side,order_id,amount,price,fee,raw) VALUES(?,?,?,?,?,?,?,?)',
                      (row['ts'],row['symbol'],row['side'],row.get('order_id'),row['amount'],row['price'],row.get('fee',0),json.dumps(row,default=str)))
