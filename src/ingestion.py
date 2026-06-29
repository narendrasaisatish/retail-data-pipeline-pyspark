from pyspark.sql import SparkSession
import pandas as pd

spark = (
    SparkSession.builder
    .appName("Retail Data Pipeline")
    .master("local[*]")
    .getOrCreate()
)

file_path = "data/raw/Online Retail.xlsx"

pdf = pd.read_excel(file_path)

print("Pandas DataFrame Created")

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)
print(type(df))
df.show(5)
df.printSchema()

print("Rows:", df.count())
print("Columns:", len(df.columns))

print("Spark DataFrame Created")

spark.stop()