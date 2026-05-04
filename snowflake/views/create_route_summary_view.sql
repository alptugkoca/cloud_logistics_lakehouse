USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.v_route_summary AS
SELECT
  "route_id",
  COUNT(*) AS shipment_count,
  ROUND(AVG("cost_eur"), 2) AS avg_cost,
  ROUND(AVG("cost_per_km"), 2) AS avg_cost_per_km,
  SUM(CASE WHEN "anomaly_flag" = 1 THEN 1 ELSE 0 END) AS anomaly_count
FROM LOGISTICS_DB.GOLD.gold_cost_anomalies
GROUP BY "route_id";
