# Search for stock Symbol or Name
from postgres_connect import pg_conn


def search_stock_symbol(text):
    conn = pg_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        query = """
        SELECT * FROM fs.stock
        WHERE symbol ILIKE %s OR name ILIKE %s;
        """
        cur.execute(query, (f"%{text}%", f"%{text}%"))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print("Error searching stock symbol:", e)
        return None


# Test search_stock_symbol
if __name__ == "__main__":
    search_text = "hens"
    rows = search_stock_symbol(search_text)
    if rows:
        for row in rows:
            print(row)
    else:
        print(f"No stock found with symbol or name like '{search_text}'")
