from delta.tables import DeltaTable
from pyspark.sql.functions import col, when, max as spark_max

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("silver_batch_transformation")

    target_path = f"{Config.SILVER_DIR}/logistics_costs"

    # Read Bronze data
    df = spark.read.format("delta").load(f"{Config.BRONZE_DIR}/logistics_costs")

    # Transform to Silver
    df_silver = (
        df.select(
            col("route_id"),
            col("transport_mode"),
            col("cost_eur").cast("double"),
            col("distance_km").cast("int"),
            col("shipment_date").cast("date"),
        )
        .withColumn(
            "cost_per_km",
            when(col("distance_km") > 0, col("cost_eur") / col("distance_km")).otherwise(None),
        )
    )

    # Deduplicate source for MERGE: keep one row per business key
    df_silver_dedup = (
        df_silver.groupBy("route_id", "transport_mode", "shipment_date")
        .agg(
            spark_max("cost_eur").alias("cost_eur"),
            spark_max("distance_km").alias("distance_km"),
            spark_max("cost_per_km").alias("cost_per_km"),
        )
    )

    # Incremental MERGE into Silver Delta table
    if DeltaTable.isDeltaTable(spark, target_path):
        delta_table = DeltaTable.forPath(spark, target_path)

        (
            delta_table.alias("target")
            .merge(
                df_silver_dedup.alias("source"),
                """
                target.route_id = source.route_id
                AND target.transport_mode = source.transport_mode
                AND target.shipment_date = source.shipment_date
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Silver batch transformation merged incrementally.")

    else:
        (
            df_silver_dedup.write
            .format("delta")
            .mode("overwrite")
            .save(target_path)
        )

        print("Silver batch transformation completed with initial load.")

    spark.stop()


if __name__ == "__main__":
    run()