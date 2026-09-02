# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# Ruta base de tus archivos en Workspace
base_path = "file:/Workspace/Users/crcaceres05@gmail.com/ecommerce-data/"

raw_tables = ["raw_customers", "raw_order_items", "raw_order_payments",
              "raw_order_reviews", "raw_orders", "raw_products", "raw_sellers"]

for table in raw_tables:
    df = (spark.read
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(f"{base_path}{table}.csv"))
    
    # Metadata de ingesta - trazabilidad, clave en Bronze
    df = df.withColumn("_ingested_at", current_timestamp())
    
    # Guardamos como tabla Delta - capa Bronze
    df.write.format("delta").mode("overwrite").saveAsTable(f"bronze_{table}")
    
    print(f"✅ bronze_{table}: {df.count()} filas")