SELECT DISTINCT
    MD5(route_id) AS route_hk,
    route_id,
    CURRENT_TIMESTAMP() AS load_datetime,
    'stg_cost_anomalies' AS record_source
FROM {{ ref('stg_cost_anomalies') }}
WHERE route_id IS NOT NULL

