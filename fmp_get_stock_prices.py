# Get Stock Prices FROM Financial Modeling Prep API

# Get Outstanding Shares from Financial Modelling
import os
import requests
import pandas as pd
import datetime
from postgres_connect import pg_conn
from fmp_get_stocks import get_existing_symbols

# Get API Key
API_KEY = os.getenv("FMP_API_KEY", "API error")


# Get all stock_prices
def fmp_get_stock_prices(symbol, startdate, enddate=None):
    if enddate is None:
        enddate = datetime.datetime.today().strftime("%Y-%m-%d")
    prices_url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={startdate}&to={enddate}&apikey={API_KEY}"
    prices_response = requests.get(prices_url)

    if prices_response.status_code == 200:
        prices_list = prices_response.json()
        df_prices = pd.DataFrame(prices_list)

        # Extract historical prices
        df_hist = pd.DataFrame(prices_list["historical"])
        df_prices["date"] = df_hist["date"]
        df_prices["open"] = df_hist["open"]
        df_prices["high"] = df_hist["high"]
        df_prices["low"] = df_hist["low"]
        df_prices["close"] = df_hist["close"]
        df_prices["volume"] = df_hist["volume"]
        df_prices["change"] = df_hist["change"]
        df_prices["changePercent"] = df_hist["changePercent"]
        df_prices["vwap"] = df_hist["vwap"]
        df_hist["symbol"] = symbol  # Add symbol column
        print(f"STOCK_PRICES list retrieved! {len(df_prices)} price dates.")
    else:
        print(
            f"Error fetching STOCK_PRICES list! Status code: {prices_response.status_code}"
        )
        df_prices = pd.DataFrame()  # Empty DataFrame as fallback

    return df_prices


# Insert stock prices
def insert_stock_prices(df):
    conn = pg_conn()
    if conn is None:
        return

    try:
        cur = conn.cursor()

        existing_stocks = get_existing_symbols()
        existing_stocks_dict = dict(zip(existing_stocks[1], existing_stocks[0]))
        count_i = 0

        # Ensure date formatting is consistent
        # df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        for _, row in df.iterrows():
            symbol = row["symbol"]

            if symbol in existing_stocks_dict:
                insert_query = """
                INSERT INTO fs.stock_price (id_stock, date, open_price, high_price, low_price, close_price, volume, change, change_percent, vwap)
                    SELECT id_stock, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    FROM fs.stock
                    WHERE symbol = %s
                    ON CONFLICT DO NOTHING;
                """
                cur.execute(
                    insert_query,
                    (
                        row["date"],
                        row["open"],
                        row["high"],
                        row["low"],
                        row["close"],
                        row["volume"],
                        row["change"],
                        row["changePercent"],
                        row["vwap"],
                        symbol,
                    ),
                )
                count_i += 1
        print(f"➕ Inserted: {count_i}")

        conn.commit()  # Save changes
        cur.close()
        print("✅ Import finished!")

    except Exception as e:
        print("Import error:", repr(e))
    finally:
        conn.close()


# Run the functions & Show database content
if __name__ == "__main__":
    start_date = (datetime.datetime.today() - datetime.timedelta(weeks=260)).strftime(
        "%Y-%m-%d"
    )
    df_price = fmp_get_stock_prices("AAPL", startdate=start_date)
    insert_stock_prices(df_price)
    df_price = fmp_get_stock_prices("MSFT", startdate=start_date)
    insert_stock_prices(df_price)
    df_price = fmp_get_stock_prices("GOOG", startdate=start_date)
    insert_stock_prices(df_price)
    df_price = fmp_get_stock_prices("NFLX", startdate=start_date)
    insert_stock_prices(df_price)
    df_price = fmp_get_stock_prices("TSLA", startdate=start_date)
    insert_stock_prices(df_price)

    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM fs.stock_price;")
    prices_content = cur.fetchall()
    print(f"Prices in database: {prices_content}")
    cur.close()
