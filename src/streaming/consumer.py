from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from src.common.config import Config
from src.common.spark_session import get_spark


EVENT_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
])

INCIDENT_SCHEMA = StructType([
    StructField("route_id", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def run() -> None:
    spark = get_spark("streaming_silver_consumer")

    # Bronze streaming paths
    events_input = f"{Config.BRONZE_DIR}/streaming_events"
    incidents_input = f"{Config.BRONZE_DIR}/streaming_incidents"

    checkpoint_base = "data/streaming/checkpoints_silver"

    # ---------------- EVENTS ----------------
    events_stream = (
        spark.readStream
        .format("delta")
        .load(events_input)
    )

    events_silver = (
        events_stream
        .select(
            F.col("user_id").cast("int"),
            F.lower(F.trim(F.col("event_type"))).alias("event_type"),
            F.to_timestamp(F.col("timestamp")).alias("event_timestamp"),
        )
    )

    events_query = (
        events_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/events")
        .start(f"{Config.SILVER_DIR}/streaming_events")
    )

    # ---------------- INCIDENTS ----------------
    incidents_stream = (
        spark.readStream
        .format("delta")
        .load(incidents_input)
    )

    incidents_silver = (
        incidents_stream
        .select(
            F.col("route_id"),
            F.lower(F.trim(F.col("incident_type"))).alias("incident_type"),
            F.lower(F.trim(F.col("severity"))).alias("severity"),
            F.to_timestamp(F.col("timestamp")).alias("incident_timestamp"),
        )
    )

    incidents_query = (
        incidents_silver.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/incidents")
        .start(f"{Config.SILVER_DIR}/streaming_incidents")
    )

    print("Streaming Silver consumer started.")
    print(f"Reading from {events_input} and {incidents_input}")
    print(f"Writing to {Config.SILVER_DIR}/streaming_*")

    events_query.awaitTermination()
    incidents_query.awaitTermination()


if __name__ == "__main__":
    run()