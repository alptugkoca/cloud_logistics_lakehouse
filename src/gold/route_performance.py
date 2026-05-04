from pyspark.sql import functions as F

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("gold_route_performance")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_costs")

    df_perf = (
        df.groupBy("route_id", "transport_mode")
        .agg(
            F.avg("cost_eur").alias("avg_cost"),
            F.avg("cost_per_km").alias("avg_cost_per_km"),
            F.count("*").alias("shipment_count"),
        )
        .withColumn(
            "efficiency_flag",
            F.when(F.col("avg_cost_per_km") > 4, F.lit("HIGH_COST"))
            .when(F.col("avg_cost_per_km") > 2.5, F.lit("MEDIUM_COST"))
            .otherwise(F.lit("LOW_COST")),
        )
        .orderBy(F.col("avg_cost_per_km").desc())
    )

    df_perf.show(truncate=False)

    # Delta (main storage)
    (
        df_perf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{Config.GOLD_DIR}/route_performance")
    )

    # Parquet (Athena)
    (
        df_perf.write
        .mode("overwrite")
        .parquet(f"{Config.GOLD_DIR}/route_performance_parquet")
    )

    # CSV (Power BI)
    (
        df_perf.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold_exports/route_performance")
    )

    print("Route performance data calculated and saved to Gold layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()