USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

DROP VIEW IF EXISTS LOGISTICS_DB.GOLD.v_data_quality_monitor;
DROP TABLE IF EXISTS LOGISTICS_DB.GOLD.data_quality_checks;

CREATE OR REPLACE TABLE LOGISTICS_DB.GOLD.data_quality_checks AS 
SELECT
    CURRENT_TIMESTAMP() AS check_time,
    COUNT(*) AS total_records,
    COUNT_IF("route_id" IS NULL) AS null_route_id,
    COUNT_IF("cost_eur" IS NULL) AS null_cost,
    COUNT_IF("distance_km" IS NULL) AS null_distance,
    COUNT(DISTINCT CONCAT("route_id", '_', "transport_mode", '_', "shipment_date")) AS distict_shipments,
    COUNT(*) - COUNT(DISTINCT CONCAT("route_id", '_', "transport_mode", '_', "shipment_date")) AS duplicate_shipments,
    MAX ("shipment_date") AS latest_shipment_date
FROM LOGISTICS_DB.GOLD.gold_cost_anomalies;
