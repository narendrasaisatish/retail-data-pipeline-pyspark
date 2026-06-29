from src.utils.spark_session import create_spark_session

# ---------------------------------------
# Create Spark Session
# ---------------------------------------
spark = create_spark_session("Bronze Layer")

# ---------------------------------------
# Read Landing CSV
# ---------------------------------------
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/landing/online_retail.csv")
)

print("Rows:", df.count())

# ---------------------------------------
# Write Bronze Layer as Parquet
# ---------------------------------------
output_path = "bronze/retail_transactions"

(
    df.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Bronze Layer Created Successfully!")

spark.stop()