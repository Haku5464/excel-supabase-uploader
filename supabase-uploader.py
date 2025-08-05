import pandas as pd
import re
from supabase import create_client, Client


def clean_excel_data(filepath):
    DATA_START_ROW = 10
    YEAR_COL = 1
    MONTH_COL = 5

    labels = [
        {"class": "A", "region": "Hong Kong", "col1": 7, "col2": 8, "col3": 9},
        {"class": "A", "region": "Kowloon", "col1": 10, "col2": 11, "col3": 12},
        {"class": "A", "region": "New Territories", "col1": 13, "col2": 14, "col3": 15},
        {"class": "B", "region": "Hong Kong", "col1": 16, "col2": 17, "col3": 18},
        {"class": "B", "region": "Kowloon", "col1": 19, "col2": 20, "col3": 21},
        {"class": "B", "region": "New Territories", "col1": 22, "col2": 23, "col3": 24},
        {"class": "C", "region": "Hong Kong", "col1": 25, "col2": 26, "col3": 27},
        {"class": "C", "region": "Kowloon", "col1": 28, "col2": 29, "col3": 30},
        {"class": "C", "region": "New Territories", "col1": 31, "col2": 32, "col3": 33},
        {"class": "D", "region": "Hong Kong", "col1": 34, "col2": 35, "col3": 36},
        {"class": "D", "region": "Kowloon", "col1": 37, "col2": 38, "col3": 39},
        {"class": "D", "region": "New Territories", "col1": 40, "col2": 41, "col3": 42},
        {"class": "E", "region": "Hong Kong", "col1": 43, "col2": 44, "col3": 45},
        {"class": "E", "region": "Kowloon", "col1": 46, "col2": 47, "col3": 48},
        {"class": "E", "region": "New Territories", "col1": 49, "col2": 50, "col3": 51},
    ]

    df = pd.read_excel(filepath, engine="xlrd", header=None)
    df[YEAR_COL] = df[YEAR_COL].replace(r'^\s*$', pd.NA, regex=True).ffill()
    df[MONTH_COL] = df[MONTH_COL].replace(r'^\s*$', pd.NA, regex=True).ffill()

    records = []
    for row_idx in range(DATA_START_ROW, df.shape[0]):
        try:
            year = int(float(df.iat[row_idx, YEAR_COL]))
            month = int(float(df.iat[row_idx, MONTH_COL]))
        except:
            continue

        for label in labels:
            v1 = str(df.iat[row_idx, label["col1"]]).strip() if pd.notna(df.iat[row_idx, label["col1"]]) else ""
            v2_raw = df.iat[row_idx, label["col2"]]
            v3 = str(df.iat[row_idx, label["col3"]]).strip() if pd.notna(df.iat[row_idx, label["col3"]]) else ""

            v2_str = str(v2_raw).strip()
            if v2_str == "-":
                value = 0.0
                small_trade = False
            else:
                try:
                    value = float(v2_raw)
                except:
                    continue
                small_trade = (v1 == "(") and (v3 == ")")

            records.append({
                "year": year,
                "month": month,
                "class": label["class"],
                "region": label["region"],
                "value": value,
                "small_trade": small_trade
            })

    return pd.DataFrame(records)

def upload_to_supabase(df: pd.DataFrame, url: str, key: str, table: str, test_rows: int = 0, batch_size: int = 500):
    supabase: Client = create_client(url, key)
    expected_columns = ["year", "month", "class", "region", "value", "small_trade"]

    missing_cols = [col for col in expected_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing expected columns in DataFrame: {missing_cols}")

    # Clean + null-safe copy
    df = df[expected_columns].copy()
    df = df.applymap(lambda x: None if pd.isna(x) else x)

    if test_rows:
        data_to_upload = df.head(test_rows).to_dict("records")
        print(f"Uploading {test_rows} test rows to Supabase table '{table}'...")
        result = supabase.table(table).insert(data_to_upload).execute()
        print("✅ Upload complete.")
        return result

    data = df.to_dict("records")
    total = len(data)
    print(f"Uploading all {total} records in batches of {batch_size}...")

    for i in range(0, total, batch_size):
        batch = data[i:i+batch_size]
        supabase.table(table).insert(batch).execute()
        print(f"✅ Uploaded rows {i+1} to {min(i+batch_size, total)}")

    print("✅ All data uploaded.")

def clear_supabase_table(url: str, key: str, table: str):
    supabase: Client = create_client(url, key)
    print(f"⚠️ Deleting all rows from table '{table}'...")
    response = supabase.table(table).delete().gt("year", 0).execute()
    print(f"✅ {len(response.data)} rows deleted.")


SUPABASE_URL = "https://urpdosbuqlnyzpbjotkl.supabase.co"    
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVycGRvc2J1cWxueXpwYmpvdGtsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NDcwMzg4NDksImV4cCI6MjA2MjYxNDg0OX0.0I8MFhZYHQvjpA2DrQ8NzEVvMDoAII0BgLCNYrqcD3g"             
TABLE_NAME1 = "flagship_HK_private_domestic_prices"
TABLE_NAME2 = "flagship_HK_private_domestic_rents"


filepath = "./data/His Data.xls"

clear_supabase_table(SUPABASE_URL, SUPABASE_KEY, TABLE_NAME2)



# df_cleaned = clean_excel_data1(filepath)

# # Preview the first few rows
# print(df_cleaned.head(10))

# # Optionally check data types
# print(df_cleaned.dtypes)

# df_cleaned.to_csv("./data/cleaned_supabase_preview2.csv", index=False)



df_cleaned = clean_excel_data(filepath)

upload_to_supabase(
    df_cleaned,
    url=SUPABASE_URL,
    key=SUPABASE_KEY,
    table=TABLE_NAME2,
    batch_size=300
)

