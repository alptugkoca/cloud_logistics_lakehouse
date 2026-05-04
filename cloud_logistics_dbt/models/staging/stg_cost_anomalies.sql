SELECT
    "route_id" AS route_id,
    "transport_mode" AS transport_mode,
    "cost_eur" AS cost_eur,
    "distance_km" AS distance_km,
    "shipment_date" AS shipment_date,
    "cost_per_km" AS cost_per_km,
    "threshold" AS threshold,
    "anomaly_flag" AS anomaly_flag,
    "anomaly_reason" AS anomaly_reason
FROM {{ source('gold', 'gold_cost_anomalies') }}
