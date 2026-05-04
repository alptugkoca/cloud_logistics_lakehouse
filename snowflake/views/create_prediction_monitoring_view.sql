USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.v_prediction_monitoring AS
SELECT
  "route_id",
  "transport_mode",
  "actual_cost_eur",
  "predicted_cost_eur",
  "prediction_error",
  "error_percentage",
  "high_error_flag"
FROM LOGISTICS_DB.GOLD.cost_predictions;
