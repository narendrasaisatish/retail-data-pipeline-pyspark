from src.utils.spark_session import create_spark_session
from pyspark.sql.functions import col, count, when

# ---------------------------------------------------
# Create Spark Session
# ---------------------------------------------------
spark = create_spark_session("Retail Data Profiling")

# ---------------------------------------------------
# Read CSV from Landing Layer
# ---------------------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/landing/online_retail.csv")
)

# ---------------------------------------------------
# Basic Information
# ---------------------------------------------------
print("=" * 60)
print("BASIC DATASET INFORMATION")
print("=" * 60)

print(f"Rows       : {df.count()}")
print(f"Columns    : {len(df.columns)}")
print(f"Partitions : {df.rdd.getNumPartitions()}")

# ---------------------------------------------------
# Schema
# ---------------------------------------------------
print("\n" + "=" * 60)
print("DATASET SCHEMA")
print("=" * 60)

df.printSchema()

# ---------------------------------------------------
# Sample Records
# ---------------------------------------------------
print("\n" + "=" * 60)
print("FIRST 5 RECORDS")
print("=" * 60)

df.show(5, truncate=False)

# ---------------------------------------------------
# Null Value Analysis
# ---------------------------------------------------
print("\n" + "=" * 60)
print("NULL VALUE ANALYSIS")
print("=" * 60)

null_counts = df.select([
    count(when(col(column).isNull(), column)).alias(column)
    for column in df.columns
])

null_counts.show()

# ---------------------------------------------------
# Duplicate Analysis
# ---------------------------------------------------
print("\n" + "=" * 60)
print("DUPLICATE ANALYSIS")
print("=" * 60)

total_rows = df.count()
distinct_rows = df.distinct().count()

print(f"Total Rows      : {total_rows}")
print(f"Distinct Rows   : {distinct_rows}")
print(f"Duplicate Rows  : {total_rows - distinct_rows}")

# ---------------------------------------------------
# Negative Quantity
# ---------------------------------------------------
print("\n" + "=" * 60)
print("NEGATIVE QUANTITY")
print("=" * 60)

negative_quantity = df.filter(col("Quantity") < 0)

print(f"Negative Quantity Rows : {negative_quantity.count()}")

# ---------------------------------------------------
# Negative Unit Price
# ---------------------------------------------------
print("\n" + "=" * 60)
print("NEGATIVE UNIT PRICE")
print("=" * 60)

negative_price = df.filter(col("UnitPrice") < 0)

print(f"Negative Price Rows : {negative_price.count()}")

# ---------------------------------------------------
# Cancelled Invoices
# ---------------------------------------------------
print("\n" + "=" * 60)
print("CANCELLED INVOICES")
print("=" * 60)

cancelled = df.filter(col("InvoiceNo").startswith("C"))

print(f"Cancelled Invoice Rows : {cancelled.count()}")

# ---------------------------------------------------
# Countries
# ---------------------------------------------------
print("\n" + "=" * 60)
print("COUNTRY ANALYSIS")
print("=" * 60)

country_count = df.select("Country").distinct().count()

print(f"Distinct Countries : {country_count}")

print("\nCountries:")

df.select("Country") \
    .distinct() \
    .orderBy("Country") \
    .show(truncate=False)

# ---------------------------------------------------
# Stop Spark
# ---------------------------------------------------
spark.stop()