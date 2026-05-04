USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.alerts_view AS
SELECT
  cp."route_id",
  cp."transport_mode",
  cp."actual_cost_eur",
  cp."predicted_cost_eur",
  cp."error_percentage",
  cp."high_error_flag",
  ca."anomaly_flag",
  ca."anomaly_reason"
FROM LOGISTICS_DB.GOLD.cost_predictions cp
LEFT JOIN LOGISTICS_DB.GOLD.gold_cost_anomalies ca
  ON cp."route_id" = ca."route_id"
 AND cp."transport_mode" = ca."transport_mode"
WHERE cp."high_error_flag" = TRUE
   OR ca."anomaly_flag" = 1;
