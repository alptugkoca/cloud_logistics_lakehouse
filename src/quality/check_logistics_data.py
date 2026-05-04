from pyspark.sql import functions as F

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("quality_logistics")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_costs")

    # 1. NULL CHECKS

    null_count = df.filter(
        F.col("route_id").isNull() |
        F.col("transport_mode").isNull() |
        F.col("distance_km").isNull() |
        F.col("cost_eur").isNull()
    ).count()

    # 2. INVALID VALUES

    invalid_cost = df.filter(F.col("cost_eur") <= 0).count()
    invalid_distance = df.filter(F.col("distance_km") <= 0).count()

    # 3. OUTLIERS (simple stats)

    stats = df.select(
        F.mean("cost_eur").alias("mean_cost"),
        F.stddev("cost_eur").alias("std_cost")
    ).collect()[0]

    mean_cost = stats["mean_cost"]
    std_cost = stats["std_cost"]

    # 4. DUPLICATES

    duplicate_count = df.groupBy(df.columns).count().filter("count > 1").count()


    # RESULT DATASET

    quality_results = [{
        "null_count": null_count,
        "invalid_cost": invalid_cost,
        "invalid_distance": invalid_distance,
        "duplicate_count": duplicate_count,
        "mean_cost": float(mean_cost),
        "std_cost": float(std_cost)
    }]

    quality_df = spark.createDataFrame(quality_results).coalesce(1)

    quality_df.show()

    # SAVE

    quality_df.write.mode("overwrite").parquet(
        f"{Config.GOLD_DIR}/data_quality_checks"
    )

    print("Data quality checks completed successfully.")

    spark.stop()


if __name__ == "__main__":
    run()