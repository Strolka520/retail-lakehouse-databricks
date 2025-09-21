# PURPOSE: Clean types, handle nulls/dedup, basic PK/FK checks. Write to Silver Delta tables.

from pyspark.sql import functions as F
from pyspark.sql import types as T

bronze_db = "retail_bronze"
silver_db = "retail_silver"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {silver_db}")

# Example schema adjustments - tweak to your dataset:
orders = spark.table(f"{bronze_db}.orders_raw")
orders_clean = (orders
    # TODO(Copilot): parse to_date for order_purchase_timestamp, order_approved_at, etc. if present
    .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
    .dropDuplicates(["order_id"])
)

order_items = spark.table(f"{bronze_db}.order_items_raw")
order_items_clean = (order_items
    .withColumn("price", F.col("price").cast("decimal(18,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(18,2)"))
)

customers = spark.table(f"{bronze_db}.customers_raw").dropDuplicates(["customer_id"])
products  = spark.table(f"{bronze_db}.products_raw").dropDuplicates(["product_id"])

payments = spark.table(f"{bronze_db}.payments_raw")
payments_clean = (payments
    .withColumn("payment_value", F.col("payment_value").cast("decimal(18,2)"))
)

def save(df, name):
    (df.write
      .format("delta")
      .mode("overwrite")
      .option("overwriteSchema", "true")
      .saveAsTable(f"{silver_db}.{name}"))

save(orders_clean, "orders")
save(order_items_clean, "order_items")
save(customers, "customers")
save(products, "products")
save(payments_clean, "payments")
