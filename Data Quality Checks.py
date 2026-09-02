# Databricks notebook source
# ============================================
# Data Quality Checks: Bronze → Silver → Gold
# ============================================

from pyspark.sql.functions import col

quality_results = []

def check(name, condition, detail=""):
    status = "✅ PASS" if condition else "❌ FAIL"
    quality_results.append((name, status, detail))
    print(f"{status} | {name} {detail}")

# --- Checks Silver: customers ---
null_customer_ids = spark.table("silver_customers").filter(col("customer_id").isNull()).count()
check("silver_customers: no nulls en customer_id", null_customer_ids == 0, f"(nulls={null_customer_ids})")

dup_customers = spark.table("silver_customers").groupBy("customer_id").count().filter("count > 1").count()
check("silver_customers: sin duplicados", dup_customers == 0, f"(dups={dup_customers})")

# --- Checks Silver: order_items (precios válidos) ---
invalid_prices = spark.table("silver_order_items").filter(col("price") <= 0).count()
check("silver_order_items: todos los precios > 0", invalid_prices == 0, f"(inválidos={invalid_prices})")

# --- Checks Gold: fact_orders (integridad referencial) ---
orphan_orders = (spark.table("gold_fact_orders").alias("f")
    .join(spark.table("gold_dim_customer").alias("c"), "customer_id", "left_anti")
    .count())
check("gold_fact_orders: sin customer_id huérfanos", orphan_orders == 0, f"(huérfanos={orphan_orders})")

orphan_products = (spark.table("gold_fact_orders").alias("f")
    .join(spark.table("gold_dim_product").alias("p"), "product_id", "left_anti")
    .count())
check("gold_fact_orders: sin product_id huérfanos", orphan_products == 0, f"(huérfanos={orphan_products})")

# --- Check Gold: delivery_days coherente (no negativos) ---
negative_delivery = spark.table("gold_fact_orders").filter(col("delivery_days") < 0).count()
check("gold_fact_orders: sin delivery_days negativos", negative_delivery == 0, f"(negativos={negative_delivery})")

# --- Resumen final ---
from pyspark.sql import Row
results_df = spark.createDataFrame([Row(check=r[0], status=r[1], detail=r[2]) for r in quality_results])
display(results_df)