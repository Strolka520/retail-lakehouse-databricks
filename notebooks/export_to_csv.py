# PURPOSE: Export Gold table to CSV for Power BI or other BI tool ingestion.
# List of tables to export
tables = ["d_customer", "d_product", "d_calendar", "f_sales"]

output_path = "/dbfs/FileStore/retail/"

for tbl in tables:
    df = spark.table(f"gold.{tbl}")
    out_file = f"{output_path}{tbl}.csv"
    (df.limit(100000)  # limit if the dataset is huge, adjust/remove as needed
       .toPandas()
       .to_csv(out_file, index=False))
    print(f"Exported {tbl} to {out_file}")
