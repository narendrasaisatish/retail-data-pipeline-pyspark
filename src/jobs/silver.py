from src.utils.spark_session import create_spark_session
from src.config.config import BRONZE_PATH, SILVER_PATH
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


spark = create_spark_session("Retail Data Pipeline - Silver Layer")


# ============================================================
# Read Bronze Layer
# ============================================================



df = spark.read.parquet(str(BRONZE_PATH))

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

silver_df.write.mode("overwrite").parquet(str(SILVER_PATH))

print("\n" + "=" * 60)
print("Silver Layer Written Successfully!")
print(f"Location : {SILVER_PATH}")
print("=" * 60)

# ============================================================
# Stop Spark
# ============================================================

spark.stop()