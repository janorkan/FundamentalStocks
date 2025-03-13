from postgres_connect import pg_conn

cur = pg_conn().cursor()

test1 = """SELECT id_stock FROM fs.stock WHERE symbol = 'AAPL' ;"""
test2 = """SELECT * FROM fs.outstanding_shares WHERE id_stock = 5328;"""
test3 = """SELECT count(*) FROM fs.company_profile WHERE id_stock = 5328;"""
test4 = """SELECT count(id_stock_price) FROM fs.stock_price WHERE id_stock = 5328;"""

cur.execute(test1)
rows = cur.fetchall()
print("test1", rows)

cur.execute(test2)
rows = cur.fetchall()
print("test2", rows)

cur.execute(test3)
rows = cur.fetchall()
print("test3", rows)

cur.execute(test4)
rows = cur.fetchall()
print("test3", rows)
