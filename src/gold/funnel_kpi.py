from pyspark.sql.functions import col, count, round, when

from src.common.config import Config
from src.common.spark_session import get_spark


def run() -> None:
    spark = get_spark("gold_funnel_kpi_calculation")

    df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/events_data")

    df_kpi = df.groupBy().agg(
        count(when(col("event_type") == "page_view", True)).alias("page_views"),
        count(when(col("event_type") == "add_to_cart", True)).alias("add_to_cart"),
        count(when(col("event_type") == "checkout", True)).alias("checkout"),
        count(when(col("event_type") == "purchase", True)).alias("purchase"),
    )

    df_kpi = (
        df_kpi
        .withColumn(
            "conversion_rate",
            when(col("page_views") > 0, col("purchase") / col("page_views")).otherwise(0),
        )
        .withColumn(
            "checkout_rate",
            when(col("page_views") > 0, col("checkout") / col("page_views")).otherwise(0),
        )
        .withColumn(
            "add_to_cart_rate",
            when(col("page_views") > 0, col("add_to_cart") / col("page_views")).otherwise(0),
        )
        .withColumn("conversion_rate", round(col("conversion_rate"), 4))
        .withColumn("checkout_rate", round(col("checkout_rate"), 4))
        .withColumn("add_to_cart_rate", round(col("add_to_cart_rate"), 4))
        .select(
            "page_views",
            "add_to_cart",
            "checkout",
            "purchase",
            "add_to_cart_rate",
            "checkout_rate",
            "conversion_rate",
        )
    )

    df_kpi.show(truncate=False)

    # Delta (main storage)
    (
        df_kpi.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{Config.GOLD_DIR}/funnel_kpi")
    )

    # Parquet (Athena)
    (
        df_kpi.write
        .mode("overwrite")
        .parquet(f"{Config.GOLD_DIR}/funnel_kpi_parquet")
    )

    # CSV (Power BI)
    (
        df_kpi.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("data/gold_exports/funnel_kpi")
    )

    print("Funnel KPI data calculated and saved to Gold layer successfully.")

    spark.stop()


if __name__ == "__main__":
    run()