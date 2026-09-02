# Databricks notebook source
from pyspark.sql.functions import col, trim, lower, to_timestamp

# --- Silver: customers ---
silver_customers = (spark.table("bronze_raw_customers")
    .dropDuplicates(["customer_id"])
    .withColumn("customer_city", lower(trim(col("customer_city"))))
    .withColumn("customer_state", trim(col("customer_state")))
    .na.drop(subset=["customer_id"]))

silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
print(f"✅ silver_customers: {silver_customers.count()} filas")

# --- Silver: orders ---
silver_orders = (spark.table("bronze_raw_orders")
    .dropDuplicates(["order_id"])
    .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
    .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
    .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date")))
    .na.drop(subset=["order_id", "customer_id"]))

silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
print(f"✅ silver_orders: {silver_orders.count()} filas")

# --- Silver: order_items ---
silver_order_items = (spark.table("bronze_raw_order_items")
    .dropDuplicates(["order_id", "order_item_id"])
    .filter(col("price") > 0)
    .na.drop(subset=["order_id", "product_id"]))

silver_order_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")
print(f"✅ silver_order_items: {silver_order_items.count()} filas")

# --- Silver: products ---
silver_products = (spark.table("bronze_raw_products")
    .dropDuplicates(["product_id"])
    .na.fill({"product_category_name": "unknown"}))

silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
print(f"✅ silver_products: {silver_products.count()} filas")

# --- Silver: sellers ---
silver_sellers = (spark.table("bronze_raw_sellers")
    .dropDuplicates(["seller_id"]))

silver_sellers.write.format("delta").mode("overwrite").saveAsTable("silver_sellers")
print(f"✅ silver_sellers: {silver_sellers.count()} filas")