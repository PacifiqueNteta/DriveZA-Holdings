import pyodbc
import pandas as pd

CONN_STRING = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=Pacifique\\SQLEXPRESS;"
    "DATABASE=DriveZA;"
    "UID=PacificNt;"
    "PWD=Paco@2404;"
)

TABLES = {
    "crm.customers":  "crm_customers.csv",
    "crm.rentals":    "crm_rentals.csv",
    "crm.promotions": "crm_promotions.csv",
    "crm.reviews":    "crm_reviews.csv",
}


def get_sql_column_types(cursor, table_name):
    schema, table = table_name.split(".", 1)
    cursor.execute(
        """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
        """,
        schema,
        table,
    )
    return {row[0]: row[1].lower() for row in cursor.fetchall()}


def normalize_value(value, sql_type):
    if value is None:
        return None

    if isinstance(value, float):
        if pd.isna(value):
            return None

    if isinstance(value, str):
        value = value.strip()
        if value == "":
            return None

    if sql_type in {"int", "smallint", "tinyint"}:
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None

    if sql_type == "decimal":
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None

    if sql_type == "bit":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))
        text = str(value).strip().lower()
        return text in {"1", "true", "yes", "y", "t"}

    if sql_type in {"date", "datetime", "datetime2"}:
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        try:
            return pd.to_datetime(str(value)).to_pydatetime()
        except Exception:
            return str(value)

    return str(value)


conn = pyodbc.connect(CONN_STRING)
cursor = conn.cursor()
cursor.fast_executemany = True

for table_name, csv_file in TABLES.items():
    df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
    column_types = get_sql_column_types(cursor, table_name)

    ordered_columns = [col for col in column_types if col in df.columns]
    missing_columns = [col for col in column_types if col not in df.columns]
    if missing_columns:
        print(f"Warning: {table_name} is missing columns in CSV: {missing_columns}")

    df = df.reindex(columns=ordered_columns)

    schema, table = table_name.split(".", 1)
    delete_sql = f"DELETE FROM [{schema}].[{table}]"
    cursor.execute(delete_sql)

    cols = ", ".join([f"[{c}]" for c in ordered_columns])
    placeholders = ", ".join(["?" for _ in ordered_columns])
    insert_sql = f"INSERT INTO [{schema}].[{table}] ({cols}) VALUES ({placeholders})"

    rows = []
    for _, row in df.iterrows():
        normalized_row = []
        for col in ordered_columns:
            normalized_row.append(normalize_value(row[col], column_types[col]))
        rows.append(tuple(normalized_row))

    cursor.executemany(insert_sql, rows)
    conn.commit()
    print(f"Loaded {table_name}: {len(rows)} rows")

cursor.close()
conn.close()
print("Done.")