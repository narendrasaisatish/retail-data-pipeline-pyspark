from pathlib import Path

# Project Root
PROJECT_ROOT = Path.home() / "databricks-projects" / "retail-data-pipeline"

# Data Paths
RAW_DATA_PATH = PROJECT_ROOT / "data" / "raw"
LANDING_PATH = PROJECT_ROOT / "data" / "landing" / "online_retail.csv"

# Medallion Layers
BRONZE_PATH = PROJECT_ROOT / "bronze" / "retail_transactions"
SILVER_PATH = PROJECT_ROOT / "silver" / "retail_transactions"
GOLD_PATH = PROJECT_ROOT / "gold"