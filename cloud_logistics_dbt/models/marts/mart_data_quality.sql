SELECT
    check_time,
    total_records,
    null_route_id,
    null_cost,
    null_distance,
    distict_shipments AS distinct_shipments,
    duplicate_shipments,
    latest_shipment_date,
    CASE
        WHEN null_route_id > 0 OR null_cost > 0 OR null_distance > 0 THEN 'DATA_ISSUE'
        WHEN duplicate_shipments > 0 THEN 'DUPLICATE_ISSUE'
        ELSE 'OK'
    END AS quality_status
FROM {{ source('gold', 'data_quality_checks') }}
