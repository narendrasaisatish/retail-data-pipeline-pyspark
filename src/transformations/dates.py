from pyspark.sql.functions import year, month, dayofmonth, hour, col


def add_date_features(df):
    """
    Add date-related features.
    """

    return (
        df
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
    )