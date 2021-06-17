SELECT 
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(arrival_time AS TIME) arrival_time,
SAFE_CAST(departure_time AS TIME) departure_time,
SAFE_CAST(stop_id AS STRING) stop_id,
SAFE_CAST(stop_sequence AS INT64) stop_sequence,
SAFE_CAST(stop_headsign AS STRING) stop_headsign,
SAFE_CAST(pickup_type AS INT64) pickup_type,
SAFE_CAST(drop_off_type AS INT64) drop_off_type,
SAFE_CAST(continuous_pickup AS INT64) continuous_pickup,
SAFE_CAST(continuous_drop_off AS INT64) continuous_drop_off,
SAFE_CAST(shape_dist_traveled AS FLOAT64) shape_dist_traveled,
SAFE_CAST(timepoint AS INT64) timepoint,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.stop_times as t