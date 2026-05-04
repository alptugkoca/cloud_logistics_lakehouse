USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.v_high_cost_routes AS
SELECT
  "route_id",
  "transport_mode",
  "avg_cost",
  "avg_cost_per_km",
  "efficiency_flag",
  "shipment_count"
FROM LOGISTICS_DB.GOLD.route_performance
WHERE "efficiency_flag" IN ('HIGH_COST', 'MEDIUM_COST');
