SELECT DISTINCT
    MD5(transport_mode) AS transport_mode_hk,
    transport_mode,
    CURRENT_TIMESTAMP() AS load_datetime,
    'stg_cost_anomalies' AS record_source
FROM {{ ref('stg_cost_anomalies') }}
WHERE transport_mode IS NOT NULL
