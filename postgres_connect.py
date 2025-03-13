# Connection with postgres database
import psycopg2
import os

password = os.getenv("PGPASSWORD", "ERROR")


def pg_conn():
    try:
        conn = psycopg2.connect(
            dbname="FundamentalStocks",
            user="postgres",
            host="localhost",
            password=password,
            port="5432",
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print("ERROR connecting to Postgres", e)
        return None


if __name__ == "__main__":
    conn = pg_conn()
    if conn:
        print("✅ Connection successfull!")
        conn.close()
    else:
        print("❌ Connection failed!")
