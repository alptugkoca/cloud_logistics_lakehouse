from pathlib import Path
import os


class Config:
    APP_ENV = os.getenv("APP_ENV", "local")

    PROJECT_ROOT = Path(__file__).resolve().parents[2]
    DATA_DIR = str(PROJECT_ROOT / "data")

    if APP_ENV == "aws":
        RAW_DIR = "s3a://cloud-logistics-lakehouse-alptug/raw"
        BRONZE_DIR = "s3a://cloud-logistics-lakehouse-alptug/bronze"
        SILVER_DIR = "s3a://cloud-logistics-lakehouse-alptug/silver"
        GOLD_DIR = "s3a://cloud-logistics-lakehouse-alptug/gold"
    else:
        RAW_DIR = f"{DATA_DIR}/raw"
        BRONZE_DIR = f"{DATA_DIR}/bronze"
        SILVER_DIR = f"{DATA_DIR}/silver"
        GOLD_DIR = f"{DATA_DIR}/gold"