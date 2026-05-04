from pyspark.sql import SparkSession
import os


def get_spark(app_name: str) -> SparkSession:
    app_env = os.getenv("APP_ENV", "local")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]))
    )

    if app_env == "aws":
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        )

    return builder.getOrCreate()
