from pathlib import Path
import joblib
import numpy as np
import json

from datetime import datetime
from delta.tables import DeltaTable
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from pyspark.sql import functions as F

from src.common.config import Config
from src.common.spark_session import get_spark

def get_current_delta_version(spark, table_path: str) -> int:
    history_df = DeltaTable.forPath(spark, table_path).history(1)
    row = history_df.select("version").collect()[0]
    return int(row["version"])

def load_training_metadata(metadata_path: Path) -> dict | None:
    if metadata_path.exists():
        with open(metadata_path, "r") as f:
            return json.load(f)
    return None

def save_training_metadata(metadata_path: Path, metadata: dict) -> None:
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

def run() -> None:
    spark = get_spark("gold_predict_cost")
    
    silver_path = f"{Config.SILVER_DIR}/logistics_costs"
    
    models_dir = Path("models")
    models_dir.mkdir(parents=True, exist_ok=True)
    
    model_path = models_dir / "cost_prediction_model.joblib"
    metadata_path = models_dir / "cost_prediction_model_metadata.json"
    
    current_version = get_current_delta_version(spark, silver_path)
    previous_metadata = load_training_metadata(metadata_path)
    
    if previous_metadata is not None: 
        previous_version = previous_metadata.get("silver_table_version", -1)
        
        if current_version <= previous_version and model_path.exists():
            print("No new Silver table data changes detected. Skipping model retraining.")
            print(f"Current Silver table version: {current_version}, Previous version in metadata: {previous_version}")
            spark.stop()
            return

    #df = spark.read.format("delta").load(f"{Config.SILVER_DIR}/logistics_costs")
    df = spark.read.format("delta").load(silver_path)
    # Feature engineering in Spark
    df = df.withColumn(
        "cost_per_km",
        F.when(F.col("distance_km") > 0, F.col("cost_eur") / F.col("distance_km")).otherwise(None)
    )
    route_avg = df.groupBy("route_id").agg(
        F.avg("cost_eur").alias("route_avg_cost")
    )

    transport_avg = df.groupBy("transport_mode").agg(
        F.avg("cost_eur").alias("transport_avg_cost")
    )

    df = df.join(route_avg, on="route_id", how="left")
    df = df.join(transport_avg, on="transport_mode", how="left")

    feature_cols = [
        "route_id",
        "transport_mode",
        "distance_km",
        "cost_per_km",
        "route_avg_cost",
        "transport_avg_cost",
    ]
    target_col = "cost_eur"

    pdf = df.select(*(feature_cols + [target_col])).dropna().toPandas()

    if pdf.empty:
        raise ValueError("No data available for training the model.")

    X = pdf[feature_cols]
    y = pdf[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    categorical_features = ["route_id", "transport_mode"]
    numerical_features = ["distance_km", "cost_per_km", "route_avg_cost", "transport_avg_cost"]

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
            ("num", "passthrough", numerical_features),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("regressor", RandomForestRegressor(n_estimators=100, random_state=42)),
        ]
    )

    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    print("Model Performance on Test Set:")
    print(f"Mean Absolute Error: {mae:.2f}")
    print(f"Mean Squared Error: {mse:.2f}")
    print(f"Root Mean Squared Error: {rmse:.2f}")
    print(f"R^2 Score: {r2:.4f}")

    pred_df = X_test.copy()
    pred_df["actual_cost_eur"] = y_test.values.round(2)
    pred_df["predicted_cost_eur"] = y_pred.round(2)
    pred_df["prediction_error"] = (
        pred_df["actual_cost_eur"] - pred_df["predicted_cost_eur"]
    ).round(2)
    pred_df["error_percentage"] = (
        pred_df["prediction_error"] / pred_df["actual_cost_eur"] * 100
    ).round(2)
    pred_df["high_error_flag"] = pred_df["error_percentage"].abs() > 30

    pred_spark_df = spark.createDataFrame(pred_df).coalesce(1)

    pred_spark_df.show(truncate=False)

    pred_spark_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{Config.GOLD_DIR}/cost_predictions")

    pred_spark_df.write.mode("overwrite").parquet(
        f"{Config.GOLD_DIR}/cost_predictions_parquet"
    )

    if Config.APP_ENV != "aws":
        pred_spark_df.write.mode("overwrite").option("header", True).csv(
            "data/gold_exports/cost_predictions"
        )

    joblib.dump(model, model_path)
    
    metrics_data = [{
        "mae": float(mae),
        "mse": float(mse),
        "rmse": float(rmse),
        "r2": float(r2)}]
    metrics_spark_df = spark.createDataFrame(metrics_data).coalesce(1)
    
    metrics_spark_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).save(f"{Config.GOLD_DIR}/cost_model_metrics_delta")

    metrics_spark_df.write.mode("overwrite").parquet(
        f"{Config.GOLD_DIR}/cost_model_metrics"
    )
    
    training_metadata = {
        "silver_table_version": current_version,
        "row_count": int(len(pdf)),
        "trained_at": datetime.utcnow().isoformat(),
        "mae": float(mae),
        "mse": float(mse),
        "rmse": float(rmse),
        "r2": float(r2),
        "model_path": str(model_path)
    }

    save_training_metadata(metadata_path, training_metadata)

    print("Prediction layer created successfully with model saved.")
    print(f"Saved model metadata for Silver version: {current_version}")

    spark.stop()


if __name__ == "__main__":
    run()