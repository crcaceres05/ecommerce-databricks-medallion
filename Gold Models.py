# Databricks notebook source
from pyspark.sql.functions import col, sum as _sum, count, avg, datediff, round as _round

# --- dim_customer ---
dim_customer = spark.table("silver_customers").select(
    "customer_id", "customer_city", "customer_state"
)
dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold_dim_customer")

# --- dim_product ---
dim_product = spark.table("silver_products").select(
    "product_id", "product_category_name"
)
dim_product.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")

# --- dim_seller ---
dim_seller = spark.table("silver_sellers").select(
    "seller_id", "seller_city", "seller_state"
)
dim_seller.write.format("delta").mode("overwrite").saveAsTable("gold_dim_seller")

# --- fact_orders: la tabla de hechos, une orders + order_items ---
orders = spark.table("silver_orders")
items = spark.table("silver_order_items")

fact_orders = (items
    .join(orders, "order_id", "inner")
    .withColumn(
        "delivery_days",
        datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
    )
    .select(
        "order_id", "order_item_id", "customer_id", "product_id", "seller_id",
        "price", "freight_value", "order_purchase_timestamp",
        "order_delivered_customer_date", "delivery_days"
    ))

fact_orders.write.format("delta").mode("overwrite").saveAsTable("gold_fact_orders")
print(f"✅ gold_fact_orders: {fact_orders.count()} filas")

# --- Métrica de negocio de ejemplo: resumen por categoría ---
sales_by_category = (fact_orders
    .join(dim_product, "product_id")
    .groupBy("product_category_name")
    .agg(
        _sum("price").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        _round(avg("delivery_days"), 1).alias("avg_delivery_days")
    )
    .orderBy(col("total_revenue").desc()))

sales_by_category.write.format("delta").mode("overwrite").saveAsTable("gold_sales_by_category")
display(sales_by_category)