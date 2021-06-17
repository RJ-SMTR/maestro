SELECT 
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(start_time AS TIME) start_time,
SAFE_CAST(end_time AS TIME) end_time,
SAFE_CAST(headway_secs AS INT64) headway_secs,
SAFE_CAST(exact_times AS INT64) exact_times,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.frequencies as t