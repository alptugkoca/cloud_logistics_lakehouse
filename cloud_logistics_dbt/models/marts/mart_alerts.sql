SELECT
    cp.route_id,
    cp.transport_mode,
    cp.actual_cost_eur,
    cp.predicted_cost_eur,
    cp.error_percentage,
    cp.high_error_flag,
    ca.anomaly_flag,
    ca.anomaly_reason,

    CASE 
        WHEN cp.error_percentage > 50 THEN 'HIGH'
        WHEN cp.error_percentage > 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS alert_severity

FROM {{ ref('stg_cost_predictions') }} cp
LEFT JOIN {{ ref('stg_cost_anomalies') }} ca
    ON cp.route_id = ca.route_id
   AND cp.transport_mode = ca.transport_mode

WHERE cp.high_error_flag = TRUE
   OR ca.anomaly_flag = 1