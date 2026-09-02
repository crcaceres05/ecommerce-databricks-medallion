# Ecommerce Databricks Medallion Pipeline

Pipeline de datos end-to-end sobre el dataset público de [Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), implementado en **Databricks** siguiendo arquitectura **Medallion (Bronze → Silver → Gold)**, con validación de calidad de datos entre capas y un dashboard analítico final.

Este proyecto es la reimplementación en Databricks/PySpark del pipeline original en [ecommerce-dataops-pipeline](https://github.com/crcaceres05/ecommerce-dataops-pipeline) (dbt + DuckDB + Airflow + Great Expectations) — mismo dataset, dos stacks distintos, para demostrar adaptabilidad a diferentes herramientas del ecosistema de datos.

## Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌───────────┐
│   Bronze    │ ──> │   Silver    │ ──> │    Gold     │ ──> │ Dashboard │
│  (raw CSV)  │     │  (cleaned)  │     │ (star schema)│    │ (Lakeview)│
└─────────────┘     └─────────────┘     └─────────────┘     └───────────┘
```

- **Bronze**: ingesta de 7 tablas CSV crudas (customers, orders, order_items, order_payments, order_reviews, products, sellers) a tablas Delta, sin transformar, con metadata de trazabilidad (`_ingested_at`).
- **Silver**: limpieza y estandarización — deduplicación, manejo de nulls, normalización de tipos y timestamps, filtrado de valores inválidos (ej. precios ≤ 0).
- **Gold**: modelado dimensional (Star Schema) — tabla de hechos `fact_orders` + dimensiones `dim_customer`, `dim_product`, `dim_seller`, más una tabla agregada de métricas de negocio por categoría.
- **Data Quality**: 6 checks automatizados entre capas (integridad referencial, nulls, duplicados, rangos válidos) — ver `notebooks/data_quality_checks.ipynb`.
- **Dashboard**: Databricks SQL / Lakeview con revenue por categoría de producto y distribución de órdenes por estado.

## Stack técnico

- **Databricks Community Edition** (Serverless SQL Warehouse)
- **PySpark** + **Delta Lake**
- **Databricks SQL / Lakeview** para visualización
- **Python 3** / **DuckDB** (extracción inicial desde el proyecto original)

## Estructura del repo

```
ecommerce-databricks-medallion/
├── notebooks/
│   ├── bronze_ingestion.ipynb
│   ├── silver_transform.ipynb
│   ├── gold_models.ipynb
│   └── data_quality_checks.ipynb
├── screenshots/
│   └── dashboard.png
└── README.md
```

## Resultados de calidad de datos

| Check | Resultado |
|---|---|
| Nulls en `customer_id` (Silver) | ✅ 0 |
| Duplicados en `customer_id` (Silver) | ✅ 0 |
| Precios inválidos (≤0) en `order_items` | ✅ 0 |
| `customer_id` huérfanos en `fact_orders` | ✅ 0 |
| `product_id` huérfanos en `fact_orders` | ✅ 0 |
| `delivery_days` negativos | ✅ 0 |

Pipeline validado end-to-end sin pérdida de filas entre capas (112,650 filas consistentes desde Bronze hasta Gold en `order_items` → `fact_orders`).

## Dashboard

![Dashboard](screenshots/dashboard.png)

Insights destacados:
- `beleza_saude`, `relogios_presentes` y `cama_mesa_banho` lideran en revenue total.
- São Paulo (SP) concentra ~42% de las órdenes totales, consistente con su peso poblacional/económico en Brasil.
- Tiempo de entrega promedio: 10-14 días en la mayoría de categorías, con outliers como `moveis_escritorio` en ~21 días — punto de investigación de negocio interesante.

## Decisiones técnicas

- **Por qué Medallion**: separación clara de responsabilidades por capa permite reprocesar desde cualquier punto sin recorrer todo el pipeline, y facilita auditar exactamente dónde se aplicó cada transformación.
- **Por qué Delta Lake**: versionado automático de tablas y soporte nativo de ACID transactions sobre Parquet, estándar de facto en Databricks.
- **Validación de calidad como notebook separado** (en vez de embebida en cada transformación): permite correr los checks de forma independiente y reutilizarlos como step de CI/CD a futuro.

## Proyecto relacionado

[ecommerce-dataops-pipeline](https://github.com/crcaceres05/ecommerce-dataops-pipeline) — mismo dataset, implementado con dbt + DuckDB + Airflow + Great Expectations.
