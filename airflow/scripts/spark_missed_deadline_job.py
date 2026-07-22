from pathlib import Path
from pyspark.sql import SparkSession


# ==================================================
# OLIST E-COMMERCE BIG DATA ETL PIPELINE
# PySpark + Spark SQL
# ==================================================


# --------------------------------------------------
# 1. Define project paths
# --------------------------------------------------

# Docker project root.
# The Data folder is mounted in Docker at:
# /opt/airflow/Data

PROJECT_ROOT = Path("/opt/airflow")

DATA_DIR = PROJECT_ROOT / "Data" / "olist"

OUTPUT_DIR = (
    PROJECT_ROOT
    / "Data"
    / "output"
    / "missed_shipping_limit_orders"
)

OUTPUT_FILE = OUTPUT_DIR / "late_shipments.csv"


# --------------------------------------------------
# 2. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("OlistDeliveryDeadlineETL")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("\n========================================")
print("OLIST E-COMMERCE ETL PIPELINE")
print("========================================")

print("\nSpark session started successfully.")


# --------------------------------------------------
# 3. Define input datasets
# --------------------------------------------------

items_path = (
    DATA_DIR
    / "olist_order_items_dataset.csv"
)

orders_path = (
    DATA_DIR
    / "olist_orders_dataset.csv"
)

products_path = (
    DATA_DIR
    / "olist_products_dataset.csv"
)


# --------------------------------------------------
# 4. Check whether input files exist
# --------------------------------------------------

required_files = [
    items_path,
    orders_path,
    products_path
]

for file_path in required_files:

    if not file_path.exists():

        print(
            f"\nERROR: Dataset not found:\n{file_path}"
        )

        spark.stop()

        raise FileNotFoundError(
            f"Required dataset missing: {file_path}"
        )


print("\nAll required datasets found.")


# --------------------------------------------------
# 5. Load datasets using PySpark
# --------------------------------------------------

print("\nLoading datasets with PySpark...")


df_items = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(items_path))
)


df_orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(orders_path))
)


df_products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(products_path))
)


print("\nDatasets loaded successfully.")


# --------------------------------------------------
# 6. Display source dataset counts
# --------------------------------------------------

items_count = df_items.count()

orders_count = df_orders.count()

products_count = df_products.count()


print("\nSOURCE DATA SUMMARY")
print("----------------------------------------")

print(
    f"Order Items : {items_count:,}"
)

print(
    f"Orders      : {orders_count:,}"
)

print(
    f"Products    : {products_count:,}"
)


# --------------------------------------------------
# 7. Create Spark SQL temporary views
# --------------------------------------------------

df_items.createOrReplaceTempView(
    "items"
)

df_orders.createOrReplaceTempView(
    "orders"
)

df_products.createOrReplaceTempView(
    "products"
)


print(
    "\nSpark SQL temporary views created."
)


# --------------------------------------------------
# 8. Spark SQL ETL Transformation
# --------------------------------------------------

# Business problem:
#
# Identify orders where sellers failed to hand
# products to the carrier before the required
# shipping deadline.
#
# Three datasets are joined:
#
# order_items
#      +
# orders
#      +
# products


print(
    "\nRunning Spark SQL ETL transformation..."
)


late_carrier_deliveries = spark.sql(
    """

    SELECT

        i.order_id,

        i.seller_id,

        i.shipping_limit_date,

        i.price,

        i.freight_value,

        p.product_id,

        p.product_category_name,

        o.customer_id,

        o.order_status,

        o.order_purchase_timestamp,

        o.order_delivered_carrier_date,

        o.order_delivered_customer_date,

        o.order_estimated_delivery_date

    FROM items AS i

    INNER JOIN orders AS o

        ON i.order_id = o.order_id

    INNER JOIN products AS p

        ON i.product_id = p.product_id

    WHERE

        o.order_delivered_carrier_date
        IS NOT NULL

        AND

        i.shipping_limit_date
        <
        o.order_delivered_carrier_date

    """
)


# --------------------------------------------------
# 9. Calculate ETL result count
# --------------------------------------------------

late_count = (
    late_carrier_deliveries.count()
)


print("\n========================================")

print(
    "ETL PROCESSING COMPLETED"
)

print("========================================")


print(
    f"\nLate shipment records found: "
    f"{late_count:,}"
)


# --------------------------------------------------
# 10. Display sample results
# --------------------------------------------------

print(
    "\nSample late shipment records:\n"
)


late_carrier_deliveries.show(
    10,
    truncate=False
)


# --------------------------------------------------
# 11. Create output directory
# --------------------------------------------------

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# --------------------------------------------------
# 12. Convert final Spark result to Pandas
# --------------------------------------------------

print(
    "\nPreparing final analytics dataset..."
)


late_shipments_pandas = (
    late_carrier_deliveries.toPandas()
)


# --------------------------------------------------
# 13. Save final cleaned dataset
# --------------------------------------------------

late_shipments_pandas.to_csv(
    OUTPUT_FILE,
    index=False
)


# --------------------------------------------------
# 14. Confirm output
# --------------------------------------------------

print("\n========================================")

print(
    "PIPELINE COMPLETED SUCCESSFULLY"
)

print("========================================")


print(
    f"\nTotal rows saved: "
    f"{len(late_shipments_pandas):,}"
)


print(
    f"\nOutput file:\n{OUTPUT_FILE}"
)


# --------------------------------------------------
# 15. Stop Spark Session
# --------------------------------------------------

spark.stop()


print(
    "\nSpark session stopped successfully."
)

print(
    "\nETL pipeline finished."
)