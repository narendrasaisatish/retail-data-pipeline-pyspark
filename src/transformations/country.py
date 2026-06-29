from pyspark.sql.functions import sum, round


def country_sales(df):
    """
    Calculate total revenue by country.
    """

    return (
        df
        .groupBy("Country")
        .agg(
            round(
                sum("Revenue"),
                2
            ).alias("TotalRevenue")
        )
        .orderBy("TotalRevenue", ascending=False)
    )