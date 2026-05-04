SELECT
    MD5(CONCAT(route_id, '|', transport_mode, '|', shipment_date, '|', cost_eur, '|', distance_km)) AS shipment_hk,

    cost_eur,
    distance_km,
    cost_per_km,
    shipment_date,
    threshold,
    anomaly_flag,
    anomaly_reason,

    MD5(CONCAT(
        COALESCE(CAST(cost_eur AS STRING), ''),
        '|',
        COALESCE(CAST(distance_km AS STRING), ''),
        '|',
        COALESCE(CAST(cost_per_km AS STRING), ''),
        '|',
        COALESCE(CAST(threshold AS STRING), ''),
        '|',
        COALESCE(CAST(anomaly_flag AS STRING), ''),
        '|',
        COALESCE(anomaly_reason, '')
    )) AS hashdiff,

    CURRENT_TIMESTAMP() AS load_datetime,
    'stg_cost_anomalies' AS record_source
FROM {{ ref('stg_cost_anomalies') }}
WHERE route_id IS NOT NULL
  AND transport_mode IS NOT NULL
  AND shipment_date IS NOT NULL
