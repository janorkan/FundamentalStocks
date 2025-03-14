# Create Tables in PostgreSQL

from postgres_connect import pg_conn


# Check if the database exists
def check_database(cur):
    cur.execute(
        """SELECT datname FROM pg_database WHERE datname = 'FundamentalStocks';"""
    )
    rows = cur.fetchall()
    if len(rows) == 0:
        print("ERROR: Database does not exist")
        exit(1)
    else:
        print("Database exists")
        print("Tables in the database: ")
        cur.execute(
            """SELECT table_name FROM information_schema.tables WHERE table_schema = 'fs';"""
        )
        rows = cur.fetchall()
        for row in rows:
            print(row)


# create tables
def create_stocks_table(cur):
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.stock (
            id_stock SERIAL PRIMARY KEY,
            symbol VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            last_price DECIMAL,
            exchange VARCHAR(255),
            exchange_short_name VARCHAR(255),
            stock_type VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
        """)
        print("Table 'fs.stock' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.stock':", e)

    # shares table
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.outstanding_shares (
            id_outstanding_shares SERIAL PRIMARY KEY,
            id_stock INT NOT NULL,
            date DATE NOT NULL,
            outstanding_shares DECIMAL NOT NULL,
            float_shares DECIMAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_stock) REFERENCES fs.stock(id_stock));
        """)
        print("Table 'fs.outstanding_shares' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.outstanding_shares':", e)

    # Stock prices table
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.stock_price (
            id_stock_price SERIAL PRIMARY KEY,
            id_stock INT NOT NULL,
            date DATE NOT NULL,
            open_price DECIMAL NOT NULL,
            high_price DECIMAL NOT NULL,
            low_price DECIMAL NOT NULL,
            close_price DECIMAL NOT NULL,
            volume BIGINT NOT NULL,
            change DECIMAL NOT NULL,
            change_percent DECIMAL NOT NULL,
            vwap DECIMAL NOT NULL,
            UNIQUE (id_stock, date),
            FOREIGN KEY (id_stock) REFERENCES fs.stock(id_stock));
        """)
        print("Table 'fs.stock_price' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.stock_price':", e)

    # Company profile table
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.company_profile (
            id_company_profile SERIAL PRIMARY KEY,
            id_stock INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            currency VARCHAR(255) NOT NULL,
            cik VARCHAR(255),
            isin VARCHAR(255),
            cusip VARCHAR(255),
            exchange VARCHAR(255),
            industry VARCHAR(255),
            website VARCHAR(255) ,
            description TEXT,
            sector VARCHAR(255),
            country VARCHAR(255),
            market_cap DECIMAL,
            employees INT,
            address VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            zip VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_stock) REFERENCES fs.stock(id_stock));
        """)
        print("Table 'fs.company_profile' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.company_profile':", e)
    conn.commit()


# Execute the functions
if __name__ == "__main__":
    conn = pg_conn()

    if conn is None:
        print("ERROR: Could not connect to the database.")
        exit(1)
    cur = conn.cursor()

    create_stocks_table(cur)

    cur.close()
    conn.close()


# Check if the database exists
if __name__ == "__main__":
    conn = pg_conn()

    if conn is None:
        print("ERROR: Could not connect to the database.")
        exit(1)
    cur = conn.cursor()

    check_database(cur)

    cur.close()
    conn.close()
