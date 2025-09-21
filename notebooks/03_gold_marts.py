# PURPOSE: Build star schema dimensions + fact table in Gold.

from pyspark.sql import functions as F

silver_db = "retail_silver"
gold_db   = "retail_gold"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

orders      = spark.table(f"{silver_db}.orders")
order_items = spark.table(f"{silver_db}.order_items")
customers   = spark.table(f"{silver_db}.customers")
products    = spark.table(f"{silver_db}.products")
payments    = spark.table(f"{silver_db}.payments")

# d_customer (current snapshot)
d_customer = (customers
    .select("customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state")
    .dropDuplicates(["customer_id"])
)
d_customer.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.d_customer")

# d_product
d_product = (products
    .select("product_id", "product_category_name", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm")
    .dropDuplicates(["product_id"])
)
d_product.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.d_product")

# d_calendar (basic)
d_calendar = (orders
    .select(F.to_date("order_purchase_timestamp").alias("date"))
    .dropna()
    .dropDuplicates(["date"])
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("dow", F.date_format("date", "E"))
)
d_calendar.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.d_calendar")

# f_sales (grain: order_id + item_id)
f_sales = (order_items.alias("oi")
    .join(orders.alias("o"), "order_id", "left")
    .join(products.alias("p"), "product_id", "left")
    .select(
        F.col("oi.order_id"),
        F.col("oi.order_item_id"),
        F.col("o.customer_id"),
        F.to_date("o.order_purchase_timestamp").alias("order_date"),
        F.col("oi.product_id"),
        F.col("oi.price").alias("item_price"),
        F.col("oi.freight_value").alias("freight"),
    )
)
f_sales.write.format("delta").mode("overwrite").saveAsTable(f"{gold_db}.f_sales")
