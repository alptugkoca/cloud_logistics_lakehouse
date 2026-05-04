USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE LOGISTICS_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW LOGISTICS_DB.GOLD.v_data_quality_monitor AS
SELECT
    *,
    CASE
        WHEN null_route_id > 0 OR null_cost > 0 OR null_distance > 0 THEN 'DATA_ISSUE'
        WHEN duplicate_shipments > 0 THEN 'DUPLICATE_ISSUE'
        ELSE 'OK'
    END AS quality_status
FROM LOGISTICS_DB.GOLD.data_quality_checks;