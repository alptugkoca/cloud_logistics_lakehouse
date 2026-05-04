from src.common.spark_session import get_spark
from src.common.config import Config


def run() -> None:
    spark = get_spark("bronze_events_ingestion")

    df_events = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{Config.RAW_DIR}/events/events_data.csv")
    )

    df_incidents = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(f"{Config.RAW_DIR}/events/logistics_incidents.csv")
    )

    print("Events Schema:")
    df_events.printSchema()

    print("Incidents Schema:")
    df_incidents.printSchema()

    (
        df_events.write
        .format("delta")
        .mode("overwrite")
        .save(f"{Config.BRONZE_DIR}/events_data")
    )

    (
        df_incidents.write
        .format("delta")
        .mode("overwrite")
        .save(f"{Config.BRONZE_DIR}/logistics_incidents")
    )

    print("Event data ingested to Bronze layer successfully.")
    print("Incident data ingested to Bronze layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()