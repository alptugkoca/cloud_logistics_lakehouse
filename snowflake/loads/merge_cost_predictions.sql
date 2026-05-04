USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

ALTER TABLE LOGISTICS_DB.GOLD.cost_predictions
ADD COLUMN IF NOT EXISTS merge_key STRING;
UPDATE LOGISTICS_DB.GOLD.cost_predictions

SET merge_key = CONCAT(
    "route_id", '_',
    "transport_mode", '_',
    "distance_km", '_',
    "actual_cost_eur", '_',
    "predicted_cost_eur"
)

WHERE merge_key IS NULL;
MERGE INTO LOGISTICS_DB.GOLD.cost_predictions AS target
USING (
  SELECT *
  FROM LOGISTICS_DB.GOLD.cost_predictions_staging
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY merge_key
    ORDER BY merge_key
  ) = 1
) AS src
ON target.merge_key = src.merge_key 

WHEN MATCHED THEN UPDATE SET 
    target."route_id" = src."route_id",
    target."transport_mode" = src."transport_mode",
    target."distance_km" = src."distance_km",
    target."cost_per_km" = src."cost_per_km",
    target."route_avg_cost" = src."route_avg_cost",
    target."transport_avg_cost" = src."transport_avg_cost",
    target."actual_cost_eur" = src."actual_cost_eur",
    target."predicted_cost_eur" = src."predicted_cost_eur",
    target."prediction_error" = src."prediction_error",
    target."error_percentage" = src."error_percentage",
    target."high_error_flag" = src."high_error_flag",
    target.merge_key = src.merge_key

WHEN NOT MATCHED THEN INSERT (
    "route_id",
    "transport_mode",
    "distance_km",
    "cost_per_km",
    "route_avg_cost",
    "transport_avg_cost",
    "actual_cost_eur",
    "predicted_cost_eur",
    "prediction_error",
    "error_percentage",
    "high_error_flag",
    merge_key
) 
VALUES (
    src."route_id",
    src."transport_mode",
    src."distance_km",
    src."cost_per_km",
    src."route_avg_cost",
    src."transport_avg_cost",
    src."actual_cost_eur",
    src."predicted_cost_eur",
    src."prediction_error",
    src."error_percentage",
    src."high_error_flag",
    src.merge_key
);
