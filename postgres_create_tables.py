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

    # Financial Report table
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.financial_report (
            id_financial_report SERIAL PRIMARY KEY,
            id_stock INT NOT NULL,
            fiscal_year INT NOT NULL,
            period INT NOT NULL,
            file_path VARCHAR(255) NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (id_stock) REFERENCES fs.stock(id_stock));
        """)
        print("Table 'fs.financial_report' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.financial_report':", e)

    # Balance sheet data
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS fs.balance_sheet (    
            id_balance_sheet SERIAL PRIMARY KEY,
            id_financial_report INT NOT NULL,
            fiscal_year INT NOT NULL,
            period INT NOT NULL,
            reported_currency VARCHAR NOT NULL,
		    cash_and_cashequivalents DECIMAL,
		    short_term_investments DECIMAL,
		    cash_and_shortterm_investments DECIMAL,
		    net_receivables DECIMAL,
		    accounts_receivables DECIMAL,
		    other_receivables DECIMAL,
		    inventory DECIMAL,
		    prepaids DECIMAL,
		    other_current_assets DECIMAL,
		    total_current_assets DECIMAL,
		    property_plant_equipment_net DECIMAL,
		    goodwill DECIMAL,
		    intangible_assets DECIMAL,
		    goodwill_and_intangible_assets DECIMAL,
		    long_term_investments DECIMAL,
		    tax_assets DECIMAL,
		    other_noncurrent_assets DECIMAL,
		    total_noncurrent_assets DECIMAL,
		    other_assets DECIMAL,
		    total_assets DECIMAL,
		    total_payables DECIMAL,
		    account_payables DECIMAL,
		    other_payables DECIMAL,
		    accrued_expenses DECIMAL,
		    shortterm_debt DECIMAL,
		    capitallease_obligations_current DECIMAL,
		    tax_payables DECIMAL,
		    deferred_revenue DECIMAL,
		    other_current_liabilities DECIMAL,
		    total_current_liabilities DECIMAL,
		    longterm_debt DECIMAL,
		    deferred_revenue_noncurrent DECIMAL,
		    deferred_taxliabilities_noncurrent DECIMAL,
		    other_noncurrent_liabilities DECIMAL,
		    total_noncurrent_liabilities DECIMAL,
		    other_liabilities DECIMAL,
		    capitallease_obligations DECIMAL,
		    total_liabilities DECIMAL,
		    treasury_stock DECIMAL,
		    preferred_stock DECIMAL,
		    common_stock DECIMAL,
		    retained_earnings DECIMAL,
		    additional_paidin_capital DECIMAL,
		    accumulatedother_comprehensive_incomeloss DECIMAL,
		    other_totalstockholders_equity DECIMAL,
		    total_stockholders_equity DECIMAL,
		    total_equity DECIMAL,
		    minority_interest DECIMAL,
		    total_liabilities_and_totalequity DECIMAL,
		    total_investments DECIMAL,
		    total_debt DECIMAL,
		    net_debt DECIMAL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (id_financial_report, fiscal_year, period),
            FOREIGN KEY (id_financial_report) REFERENCES fs.stock(id_stock));
        """)
        print("Table 'fs.balance_sheet' created or already exists.")
    except Exception as e:
        print("Error creating table 'fs.balance_sheet':", e)

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
