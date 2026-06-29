from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, round

# =====================================================
# Create Spark Session
# =====================================================

spark = (
    SparkSession.builder
    .appName("Gold Layer")
    .master("local[*]")
    .getOrCreate()
)

# =====================================================
# Read Silver Layer
# =====================================================

silver_path = "/home/satish/databricks-projects/retail-data-pipeline/silver/retail_transactions"

df = spark.read.parquet(silver_path)

print("Rows:", df.count())

# =====================================================
# Monthly Revenue
# =====================================================

monthly_sales = (
    df.groupBy(
        "InvoiceYear",
        "InvoiceMonth"
    )
    .agg(
        round(sum("Revenue"), 2).alias("TotalRevenue")
    )
    .orderBy(
        "InvoiceYear",
        "InvoiceMonth"
    )
)

print("\nMonthly Revenue\n")

monthly_sales.show(50, truncate=False)

# =====================================================
# Write Gold Layer
# =====================================================

output_path = "/home/satish/databricks-projects/retail-data-pipeline/gold/monthly_sales"

(
    monthly_sales.write
    .mode("overwrite")
    .parquet(output_path)
)

print("\nMonthly Sales Gold Layer Created!")

spark.stop()