from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("bronze_batch_ingestion")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{Config.RAW_DIR}/batch/logistics_costs.csv")
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(f"{Config.BRONZE_DIR}/logistics_costs")
    )

    print("Batch data ingested to Bronze layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()