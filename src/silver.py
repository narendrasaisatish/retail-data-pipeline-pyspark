from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    year,
    month,
    dayofmonth,
    hour,
    when
)

# ============================================================
# Create Spark Session
# ============================================================

spark = (
    SparkSession.builder
    .appName("Retail Data Pipeline - Silver Layer")
    .master("local[*]")
    .getOrCreate()
)

# ============================================================
# Read Bronze Layer
# ============================================================

bronze_path = "/home/satish/databricks-projects/retail-data-pipeline/bronze/retail_transactions"

df = spark.read.parquet(bronze_path)

print("=" * 60)
print("BRONZE LAYER")
print("=" * 60)

print(f"Rows : {df.count()}")
print(f"Columns : {len(df.columns)}")

# ============================================================
# Business Transformations
# ============================================================

silver_df = (

    df

    # Revenue
    .withColumn(
        "Revenue",
        (col("Quantity") * col("UnitPrice")).cast("double")
    )

    # Date Features
    .withColumn(
        "InvoiceYear",
        year(col("InvoiceDate"))
    )

    .withColumn(
        "InvoiceMonth",
        month(col("InvoiceDate"))
    )

    .withColumn(
        "InvoiceDay",
        dayofmonth(col("InvoiceDate"))
    )

    .withColumn(
        "InvoiceHour",
        hour(col("InvoiceDate"))
    )

    # Cancelled Orders
    .withColumn(
        "IsCancelled",
        when(
            col("InvoiceNo").startswith("C"),
            True
        ).otherwise(False)
    )

)

print("\n" + "=" * 60)
print("SILVER SCHEMA")
print("=" * 60)

silver_df.printSchema()

print("\n" + "=" * 60)
print("FIRST 5 ROWS")
print("=" * 60)

silver_df.show(5, truncate=False)


print("\n" + "=" * 60)
print("LOGICAL & PHYSICAL EXECUTION PLAN")
print("=" * 60)

silver_df.explain(True)

# ============================================================
# Write Silver Layer
# ============================================================

silver_path = "/home/satish/databricks-projects/retail-data-pipeline/silver/retail_transactions"

(
    silver_df.write
    .mode("overwrite")
    .parquet(silver_path)
)

print("\n" + "=" * 60)
print("Silver Layer Written Successfully!")
print(f"Location : {silver_path}")
print("=" * 60)

# ============================================================
# Stop Spark
# ============================================================

spark.stop()