USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE LOGISTICS_DB.GOLD.cost_predictions_staging
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@LOGISTICS_DB.GOLD.logistics_gold_stage/cost_predictions_parquet/',
            FILE_FORMAT => 'PARQUET_FF'
        )
    )
);

TRUNCATE TABLE LOGISTICS_DB.GOLD.cost_predictions_staging;

COPY INTO LOGISTICS_DB.GOLD.cost_predictions_staging
FROM @LOGISTICS_DB.GOLD.logistics_gold_stage/cost_predictions_parquet/
FILE_FORMAT = (FORMAT_NAME = PARQUET_FF)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet';

ALTER TABLE LOGISTICS_DB.GOLD.cost_predictions_staging
ADD COLUMN IF NOT EXISTS merge_key STRING;

UPDATE LOGISTICS_DB.GOLD.cost_predictions_staging
SET merge_key = CONCAT(
    "route_id", '_',
    "transport_mode", '_',
    "distance_km", '_',
    "actual_cost_eur", '_',
    "predicted_cost_eur"
);
