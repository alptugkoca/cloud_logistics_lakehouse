from pyspark.sql.functions import avg

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("gold_logistics_kpi")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_costs")

    df_kpi = (
        df.groupBy("route_id", "transport_mode")
        .agg(
            avg("cost_eur").alias("avg_cost"),
            avg("cost_per_km").alias("avg_cost_per_km"),
        )
    )

    df_kpi.show(truncate=False)

    # Delta (main storage)
    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{Config.GOLD_DIR}/logistics_kpi")
    )

    # Parquet (Athena)
    (
        df_kpi.write
        .mode("overwrite")
        .parquet(f"{Config.GOLD_DIR}/logistics_kpi_parquet")
    )

    # CSV (Power BI)
    (
        df_kpi.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold_exports/logistics_kpi")
    )

    print("Logistics KPI data calculated and saved to Gold layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()