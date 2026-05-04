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

RAW_STREAM_SCHEMA = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("incident_type", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("timestamp", StringType(), True),
])


def run() -> None:
    spark = get_spark("streaming_bronze_ingestion")

    input_path = "data/streaming/events"
    checkpoint_base = "data/streaming/checkpoints"

    raw_stream = (
        spark.readStream
        .format("json")
        .option("maxFilesPerTrigger", 1)
        .schema(RAW_STREAM_SCHEMA)
        .load(input_path)
    )

    events_stream = (
        raw_stream
        .filter(F.col("event_type").isNotNull())
        .select("user_id", "event_type", "timestamp")
    )

    incidents_stream = (
        raw_stream
        .filter(F.col("incident_type").isNotNull())
        .select("route_id", "incident_type", "severity", "timestamp")
    )

    events_query = (
        events_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/events")
        .start(f"{Config.BRONZE_DIR}/streaming_events")
    )

    incidents_query = (
        incidents_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{checkpoint_base}/incidents")
        .start(f"{Config.BRONZE_DIR}/streaming_incidents")
    )

    print("Streaming ingestion started.")
    print(f"Writing events to {Config.BRONZE_DIR}/streaming_events")
    print(f"Writing incidents to {Config.BRONZE_DIR}/streaming_incidents")

    events_query.awaitTermination()
    incidents_query.awaitTermination()


if __name__ == "__main__":
    run()