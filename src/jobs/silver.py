from src.utils.spark_session import create_spark_session
from src.config.config import BRONZE_PATH, SILVER_PATH
from src.utils.logger import get_logger
from src.transformations.sales import add_revenue
from src.transformations.dates import add_date_features
from src.transformations.flags import add_cancel_flag


# ============================================================
# Create Spark Session
# ============================================================


spark = create_spark_session("Retail Data Pipeline - Silver Layer")


# ============================================================
# Read Bronze Layer
# ============================================================
logger = get_logger("Silver Layer")


df = spark.read.parquet(str(BRONZE_PATH))

print("=" * 60)
print("BRONZE LAYER")
print("=" * 60)

logger.info(f"Rows Loaded: {df.count()}")
print(f"Columns : {len(df.columns)}")

# ============================================================
# Business Transformations
# ============================================================

logger.info("Adding Revenue Column")
df = add_revenue(df)

logger.info("Adding Date Features")
df = add_date_features(df)

logger.info("Adding Cancel Flag")
silver_df = add_cancel_flag(df)


logger.info("Displaying Silver Schema")
print("\n" + "=" * 60)
print("SILVER SCHEMA")
print("=" * 60)

silver_df.printSchema()

print("\n" + "=" * 60)
logger.info("Displaying Sample Records")
print("=" * 60)

silver_df.show(5, truncate=False)


print("\n" + "=" * 60)
print("LOGICAL & PHYSICAL EXECUTION PLAN")
print("=" * 60)

silver_df.explain(True)

# ============================================================
# Write Silver Layer
# ============================================================

silver_df.write.mode("overwrite").parquet(str(SILVER_PATH))

print("\n" + "=" * 60)
logger.info("Silver Layer Written Successfully")
print(f"Location : {SILVER_PATH}")
print("=" * 60)

# ============================================================
# Stop Spark
# ============================================================

spark.stop()