SELECT 
SAFE_CAST(vehicle_id AS STRING) vehicle_id,
SAFE_CAST(departure_datetime AS DATETIME) departure_datetime,
SAFE_CAST(arrival_datetime AS DATETIME) arrival_datetime,
SAFE_CAST(departure_id AS STRING) departure_id,
SAFE_CAST(arrival_id AS STRING) arrival_id,
SAFE_CAST(distance AS FLOAT64) distance,
SAFE_CAST(elapsed_time AS FLOAT64) elapsed_time,
SAFE_CAST(average_speed AS FLOAT64) average_speed,
SAFE_CAST(trajectory_type AS STRING) trajectory_type,
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(trip_short_name AS STRING) trip_short_name,
SAFE_CAST(gtfs_version_date AS STRING) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_planned_staging.realized_trips_planned as t