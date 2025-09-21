# PURPOSE: Ingest raw CSVs into Delta Bronze tables.
# ACTION: Copilot, write helper functions where TODOs appear.

from pyspark.sql import functions as F
from pyspark.sql import types as T

catalog_db = "retail_bronze"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_db}")

# Config
RAW_PATH = "/FileStore/retail"  # Upload CSVs here: orders, order_items, customers, products, payments

tables = {
    "orders_raw":        {"path": f"{RAW_PATH}/orders.csv"},
    "order_items_raw":   {"path": f"{RAW_PATH}/order_items.csv"},
    "customers_raw":     {"path": f"{RAW_PATH}/customers.csv"},
    "products_raw":      {"path": f"{RAW_PATH}/products.csv"},
    "payments_raw":      {"path": f"{RAW_PATH}/payments.csv"},
}

def read_csv_to_df(path: str):
    return (spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path))

def normalize_columns(df):
    # TODO(Copilot): implement: lowercase, replace spaces/hyphens with underscores
    for c in df.columns:
        df = df.withColumnRenamed(c, c.strip().lower().replace(" ", "_").replace("-", "_"))
    return df

for table_name, cfg in tables.items():
    df = read_csv_to_df(cfg["path"])
    df = normalize_columns(df)
    full_table = f"{catalog_db}.{table_name}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(full_table))
    print(f"Wrote {full_table} ({df.count()} rows)")

