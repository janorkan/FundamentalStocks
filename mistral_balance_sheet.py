# Extract Balance Sheet Data from
import pandas as pd
import json
from mistral_connect import mistral_con
from postgres_connect import pg_conn


# Get balance sheet columns
def get_balancesheet_columns():
    conn = pg_conn()
    cur = conn.cursor()
    cur.execute("""
                SELECT column_name
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE table_name = 'balance_sheet'""")
    nested_columns = cur.fetchall()
    flattened_columns = [item[0] for item in nested_columns]
    return flattened_columns


# Let Mistral extract balance information
def mistral_balance_sheet_extract(url, symbol, year, quarter):
    mistral = mistral_con()
    model = "mistral-large-latest"
    columns = get_balancesheet_columns()
    prompt = f"Please extract the given financial statement from {symbol} for the fiscal period of {year} and quarter {quarter}. The data can be found in the Balance sheets table. The tables first column contains the variable names, the next column the values of the given fiscal year and period, the last column the values of the previous fiscal year for the same period. Match the data to the following values {columns}. Output the data as pure json so i can easily transform it into a pandas dataframe so that each of the two periods are in another row."

    chat_response = mistral.chat.complete
    model = model

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": prompt,
                },
                {
                    "type": "document_url",
                    "document_url": url,
                },
            ],
        }
    ]

    chat_response = mistral.chat.complete(
        model=model,
        messages=messages,
        response_format={
            "type": "json_object",
        },
    )

    parsed_json = json.loads(chat_response.choices[0].message.content)
    parsed_data = parsed_json["data"]

    df = pd.DataFrame(parsed_data)

    return df


# Tests
if __name__ == "__main__":
    symbol = "PYPL"
    url = "https://s205.q4cdn.com/875401827/files/doc_financials/2024/q4/PYPL-4Q-24-Earnings-Release.pdf"
    year = 2024
    quarter = 4

    df = mistral_balance_sheet_extract(url, symbol, year, quarter)
    print(df)
