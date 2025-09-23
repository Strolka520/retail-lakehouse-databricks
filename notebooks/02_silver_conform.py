# PURPOSE: Clean types, handle nulls/dedup, basic PK/FK checks. Write to Silver Delta tables.

from pyspark.sql import functions as F
from pyspark.sql import types as T

bronze_db = "bronze"
silver_db = "silver"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

bronze_tables = [t.name for t in spark.catalog.listTables(bronze_db)]

def clean_table(df, table_name):
    if "id" in df.columns:
        df = df.dropDuplicates(["id"])
    for col, dtype in df.dtypes:
        if dtype == "double":
            df = df.withColumn(col, F.col(col).cast("decimal(18,2)"))
    for col, dtype in df.dtypes:
        if dtype == "string":
            df = df.fillna({col: ""})
    return df

def save(df, name):
    (df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{silver_db}.{name}"))

for table in bronze_tables:
    df = spark.table(f"{bronze_db}.{table}")
    df_clean = clean_table(df, table)
    save(df_clean, table)
