from src.utils.spark_session import create_spark_session
import pandas as pd

spark = create_spark_session("Retail Data Pipeline")


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