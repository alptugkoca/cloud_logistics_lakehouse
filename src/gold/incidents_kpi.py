from pyspark.sql.functions import count

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("gold_incidents_kpi")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_incidents")

    df_kpi = (
        df.groupBy("incident_type", "severity")
        .agg(count("*").alias("incident_count"))
    )

    df_kpi.show(truncate=False)

    # Delta (main storage)
    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{Config.GOLD_DIR}/incidents_kpi")
    )

    # Parquet (Athena)
    (
        df_kpi.write
        .mode("overwrite")
        .parquet(f"{Config.GOLD_DIR}/incidents_kpi_parquet")
    )

    # CSV (Power BI)
    (
        df_kpi.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold_exports/incidents_kpi")
    )

    print("Incidents KPI data calculated and saved to Gold layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()