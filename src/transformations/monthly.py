from pyspark.sql.functions import sum, round


def monthly_sales(df):

    return (
        df
        .groupBy(
            "InvoiceYear",
            "InvoiceMonth"
        )
        .agg(
            round(
                sum("Revenue"),
                2
            ).alias("TotalRevenue")
        )
        .orderBy(
            "InvoiceYear",
            "InvoiceMonth"
        )
    )