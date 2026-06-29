from pyspark.sql.functions import col


def add_revenue(df):
    return df.withColumn(
        "Revenue",
        col("Quantity") * col("UnitPrice")
    )