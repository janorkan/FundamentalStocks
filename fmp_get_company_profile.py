# Get Company Profile

# Get Outstanding Shares from Financial Modelling
import os
import requests
import pandas as pd
from postgres_connect import pg_conn
from fmp_get_stocks import get_existing_symbols

# Get API Key
API_KEY = os.getenv("FMP_API_KEY", "API error")


# Get all stocks
def fmp_get_company_profile_data(symbol):
    company_profile_url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={API_KEY}"
    company_profile_response = requests.get(company_profile_url)

    if company_profile_response.status_code == 200:
        cp_list = company_profile_response.json()
        df_cp = pd.DataFrame(cp_list)
        print(f"COMPANY_PROFILE data retrieved! {len(df_cp)} share rows.")
    else:
        print(
            f"Error fetching COMPANY_PROFILE list! Status code: {company_profile_response.status_code}"
        )
        df_cp = pd.DataFrame()  # Empty DataFrame as fallback

    return df_cp


# Get all existing company profiles
def get_existing_company_profiles():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT stock.id_stock, stock.symbol FROM fs.company_profile INNER JOIN fs.stock ON fs.company_profile.id_stock = fs.stock.id_stock"
    )
    rows = cur.fetchall()
    existing_id_company_profiles = [row[0] for row in rows]
    existing_company_profiles = [row[1] for row in rows]
    cur.close()
    conn.close
    return existing_id_company_profiles, existing_company_profiles


def insert_updated_company_profile(df):
    conn = pg_conn()
    if conn is None:
        return

    try:
        cur = conn.cursor()
        existing_stocks = get_existing_symbols()
        existing_stocks_dict = dict(zip(existing_stocks[1], existing_stocks[0]))

        existing_company_profiles = get_existing_company_profiles()
        existing_company_profiles_dict = dict(
            zip(existing_company_profiles[1], existing_company_profiles[0])
        )

        count_u = 0
        count_i = 0

        for _, row in df.iterrows():
            symbol = row["symbol"]

            if symbol in existing_company_profiles_dict:
                update_query = """
                UPDATE fs.company_profile
                SET 
                    name = %s,
                    currency = %s,
                    cik = %s,
                    isin = %s,
                    cusip = %s,
                    exchange =%s,
                    industry = %s,
                    website = %s,
                    description = %s,
                    sector = %s,
                    country = %s,
                    market_cap = %s,
                    employees = %s,
                    address = %s,
                    city = %s,
                    state = %s,
                    zip = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id_stock = %s;
                """
                cur.execute(
                    update_query,
                    (
                        row["companyName"],
                        row["currency"],
                        row["cik"],
                        row["isin"],
                        row["cusip"],
                        row["exchange"],
                        row["industry"],
                        row["website"],
                        row["description"],
                        row["sector"],
                        row["country"],
                        row["marketCap"],
                        row["fullTimeEmployees"],
                        row["address"],
                        row["city"],
                        row["state"],
                        row["zip"],
                        existing_stocks_dict[symbol],
                    ),
                )
                count_u += 1

            else:
                insert_query = """
                INSERT INTO fs.company_profile (id_stock, name, currency, cik, isin, cusip, exchange, industry, website, description, sector, country, market_cap, employees, address, city, state, zip)
                    SELECT id_stock, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    FROM fs.stock
                    WHERE symbol = %s AND NOT EXISTS (
                        SELECT 1
                        FROM fs.company_profile
                        WHERE id_stock = fs.stock.id_stock
                    );
                """
                cur.execute(
                    insert_query,
                    (
                        row["companyName"],
                        row["currency"],
                        row["cik"],
                        row["isin"],
                        row["cusip"],
                        row["exchange"],
                        row["industry"],
                        row["website"],
                        row["description"],
                        row["sector"],
                        row["country"],
                        row["marketCap"],
                        row["fullTimeEmployees"],
                        row["address"],
                        row["city"],
                        row["state"],
                        row["zip"],
                        symbol,
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
    df_cp = fmp_get_company_profile_data("AAPL")
    insert_updated_company_profile(df_cp)

    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM fs.company_profile")
    cp_content = cur.fetchall()
    print(f"Shares in database: {cp_content}")
    cur.close()
