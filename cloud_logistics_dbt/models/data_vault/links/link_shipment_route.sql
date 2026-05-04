SELECT DISTINCT
    MD5(CONCAT(
        MD5(CONCAT(route_id, '|', transport_mode, '|', shipment_date, '|', cost_eur, '|', distance_km)),
        '|',
        MD5(route_id)
    )) AS shipment_route_lhk,

    MD5(CONCAT(route_id, '|', transport_mode, '|', shipment_date, '|', cost_eur, '|', distance_km)) AS shipment_hk,
    MD5(route_id) AS route_hk,

    CURRENT_TIMESTAMP() AS load_datetime,
    'stg_cost_anomalies' AS record_source
FROM {{ ref('stg_cost_anomalies') }}
WHERE route_id IS NOT NULL
  AND transport_mode IS NOT NULL
  AND shipment_date IS NOT NULL
