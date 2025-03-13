# Get all Stocks form Financial Modeling Prep API
import os
import requests
import pandas as pd
from postgres_connect import pg_conn

# Get API Key
API_KEY = os.getenv("FMP_API_KEY", "API error")


# Get all stocks
def fmp_get_all_symbols():
    stocks_url = (
        f"https://financialmodelingprep.com/api/v3/stock/list?&apikey={API_KEY}"
    )
    stocks_response = requests.get(stocks_url)

    if stocks_response.status_code == 200:
        stock_list = stocks_response.json()
        df_stocks = pd.DataFrame(stock_list)
        print(f"Stock list retrieved! {len(df_stocks)} stocks.")
    else:
        print(f"Error fetching stock list! Status code: {stocks_response.status_code}")
        df_stocks = pd.DataFrame()  # Empty DataFrame as fallback

    return df_stocks


# Get all existing stocks
def get_existing_symbols():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT id_stock, symbol FROM fs.stock")
    rows = cur.fetchall()
    existing_id_stocks = [row[0] for row in rows]
    existing_stocks_symbols = [row[1] for row in rows]
    cur.close()
    conn.close
    return existing_id_stocks, existing_stocks_symbols


# Insert or update stocks
def insert_or_update_stocks(df):
    conn = pg_conn()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        existing_stocks = get_existing_symbols()
        existing_stocks_dict = dict(zip(existing_stocks[1], existing_stocks[0]))
        count_u = 0
        count_i = 0

        for _, row in df.iterrows():
            symbol = row["symbol"]

            if symbol in existing_stocks_dict:
                update_query = """
                UPDATE fs.stock
                SET 
                    name = %s,
                    last_price = %s,
                    exchange =%s,
                    exchange_short_name = %s,
                    stock_type = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id_stock = %s;
                """
                cur.execute(
                    update_query,
                    (
                        row["name"],
                        row["price"],
                        row["exchange"],
                        row["exchangeShortName"],
                        row["type"],
                        existing_stocks_dict[symbol],
                    ),
                )
                count_u += 1

            else:
                insert_query = """
                INSERT INTO fs.stock (symbol, name, price, exchange, exchange_short_name, stock_type)
                VALUES (%s, %s, %s, %s, %s, %s);
                """
                cur.execute(
                    insert_query,
                    (
                        symbol,
                        row["name"],
                        row["exchange"],
                        row["exchangeShortName"],
                        row["type"],
                    ),
                )
                count_i += 1
        print(f"🔄 Updated: {count_u} - ➕ Inserted: {count_i}")

        conn.commit()  # Save changes
        cur.close()
        print("✅ Import finished!")

    except Exception as e:
        print("Import error:", repr(e))
    finally:
        conn.close()


# Run the functions & Show database content
if __name__ == "__main__":
    df_stocks = fmp_get_all_symbols()
    insert_or_update_stocks(df_stocks)

    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM fs.stock")
    stocks_content = cur.fetchall()
    print(f"Stocks in database: {stocks_content}")
    cur.close()
