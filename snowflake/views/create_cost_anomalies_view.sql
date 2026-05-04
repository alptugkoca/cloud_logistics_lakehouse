USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.v_cost_anomalies AS
SELECT
  "route_id",
  "transport_mode",
  "cost_eur",
  "distance_km",
  "cost_per_km",
  "threshold",
  "anomaly_flag",
  "anomaly_reason"
FROM LOGISTICS_DB.GOLD.gold_cost_anomalies
WHERE "anomaly_flag" = 1;
