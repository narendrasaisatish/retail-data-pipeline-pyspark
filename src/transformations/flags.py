from pyspark.sql.functions import when, col


def add_cancel_flag(df):
    """
    Mark cancelled invoices.
    """

    return df.withColumn(
        "IsCancelled",
        when(
            col("InvoiceNo").startswith("C"),
            True
        ).otherwise(False)
    )