from pyspark.sql.functions import col, lower, to_timestamp, trim
from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("silver_events_transform")

    # Read Bronze data
    df_events = spark.read.format("delta").load(f"{Config.BRONZE_DIR}/events_data")
    df_incidents = spark.read.format("delta").load(f"{Config.BRONZE_DIR}/logistics_incidents")

    # Transform events data
    df_events_silver = (
        df_events.select(
            col("user_id").cast("int").alias("user_id"),
            lower(trim(col("event_type"))).alias("event_type"),
            to_timestamp(col("timestamp")).alias("event_timestamp"),
        )
    )

    # Transform incidents data
    df_incidents_silver = (
        df_incidents.select(
            col("route_id"),
            lower(trim(col("incident_type"))).alias("incident_type"),
            lower(trim(col("severity"))).alias("severity"),
            to_timestamp(col("timestamp")).alias("incident_timestamp"),
        )
    )

    # Write to Silver
    (
        df_events_silver.write
        .format("delta")
        .mode("overwrite")
        .save(f"{Config.SILVER_DIR}/events_data")
    )

    (
        df_incidents_silver.write
        .format("delta")
        .mode("overwrite")
        .save(f"{Config.SILVER_DIR}/logistics_incidents")
    )

    print("Silver events and incidents transformation completed.")

    spark.stop()


if __name__ == "__main__":
    run()