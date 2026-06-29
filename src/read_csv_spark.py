from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("Retail Data Pipeline")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("data/landing/online_retail.csv")
)

print("Spark DataFrame Created")

df.show(5)
df.printSchema()

print("Rows:", df.count())
print("Columns:", len(df.columns))
print("Partitions:", df.rdd.getNumPartitions())

spark.stop()