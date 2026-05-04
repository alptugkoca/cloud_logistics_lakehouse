from pyspark.sql import functions as F

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("gold_cost_anomaly")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_costs")

    stats = df.select(
        F.mean("cost_per_km").alias("mean_cost_per_km"),
        F.stddev("cost_per_km").alias("stddev_cost_per_km"),
    ).collect()[0]

    mean_cost_per_km = stats["mean_cost_per_km"]
    stddev_cost_per_km = stats["stddev_cost_per_km"]

    if mean_cost_per_km is None:
        raise ValueError("cost_per_km statistics could not be calculated. Check Silver logistics_costs data.")

    if stddev_cost_per_km is None:
        stddev_cost_per_km = 0.0

    threshold = mean_cost_per_km + 3 * stddev_cost_per_km

    print(f"Anomaly threshold (cost_per_km): {threshold}")

    df_anomaly = (
        df.withColumn(
            "anomaly_flag",
            F.when(F.col("cost_per_km") > F.lit(threshold), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "anomaly_reason",
            F.when(F.col("anomaly_flag") == 1, F.lit("HIGH_COST_PER_KM")).otherwise(F.lit("NORMAL")),
        )
        .withColumn("threshold", F.lit(threshold))
        .filter(F.col("anomaly_flag") == 1)
    )

    df_anomaly.show(truncate=False)

    # Delta (main storage)
    (
        df_anomaly.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{Config.GOLD_DIR}/cost_anomalies")
    )

    # Parquet (Athena)
    (
        df_anomaly.write
        .mode("overwrite")
        .parquet(f"{Config.GOLD_DIR}/cost_anomalies_parquet")
    )

    # CSV (Power BI)
    (
        df_anomaly.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold_exports/cost_anomalies")
    )

    print("Cost anomaly data calculated and saved to Gold layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()