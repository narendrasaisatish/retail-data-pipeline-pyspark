from src.utils.spark_session import create_spark_session
from src.utils.logger import get_logger

from src.config.config import SILVER_PATH, GOLD_PATH

from src.transformations.monthly import monthly_sales
from src.transformations.country import country_sales


# =====================================================
# Create Spark Session
# =====================================================

spark = create_spark_session("Gold Layer")
logger = get_logger("Gold Layer")


# =====================================================
# Read Silver Layer
# =====================================================

logger.info("Reading Silver Layer")

df = spark.read.parquet(str(SILVER_PATH))

logger.info(f"Rows Loaded: {df.count()}")
logger.info(f"Columns Loaded: {len(df.columns)}")


# =====================================================
# Create Monthly Revenue Gold Table
# =====================================================

logger.info("Creating Monthly Revenue")

monthly_df = monthly_sales(df)

logger.info("Displaying Monthly Revenue")

monthly_df.show(50, truncate=False)


# =====================================================
# Create Country Revenue Gold Table
# =====================================================

logger.info("Creating Country Revenue")

country_df = country_sales(df)

logger.info("Displaying Country Revenue")

country_df.show(50, truncate=False)


# =====================================================
# Write Monthly Revenue
# =====================================================

logger.info("Writing Monthly Revenue Gold Table")

monthly_output = str(GOLD_PATH / "monthly_sales")

(
    monthly_df.write
    .mode("overwrite")
    .parquet(monthly_output)
)


# =====================================================
# Write Country Revenue
# =====================================================

logger.info("Writing Country Revenue Gold Table")

country_output = str(GOLD_PATH / "country_sales")

(
    country_df.write
    .mode("overwrite")
    .parquet(country_output)
)


# =====================================================
# Job Completed
# =====================================================

logger.info("Gold Layer Created Successfully")
logger.info("Stopping Spark Session")

spark.stop()