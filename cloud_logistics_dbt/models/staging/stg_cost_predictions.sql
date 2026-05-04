SELECT
    "route_id" AS route_id,
    "transport_mode" AS transport_mode,
    "distance_km" AS distance_km,
    "cost_per_km" AS cost_per_km,
    "route_avg_cost" AS route_avg_cost,
    "transport_avg_cost" AS transport_avg_cost,
    "actual_cost_eur" AS actual_cost_eur,
    "predicted_cost_eur" AS predicted_cost_eur,
    "prediction_error" AS prediction_error,
    "error_percentage" AS error_percentage,
    "high_error_flag" AS high_error_flag,
    merge_key
FROM {{ source('gold', 'cost_predictions') }}
