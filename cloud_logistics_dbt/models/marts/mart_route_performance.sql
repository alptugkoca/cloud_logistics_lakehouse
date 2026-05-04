SELECT
    "route_id" AS route_id,
    "transport_mode" AS transport_mode,
    "avg_cost" AS avg_cost,
    "avg_cost_per_km" AS avg_cost_per_km,
    "efficiency_flag" AS efficiency_flag,
    "shipment_count" AS shipment_count
FROM {{ source('gold', 'route_performance') }}
WHERE "route_id" IS NOT NULL
