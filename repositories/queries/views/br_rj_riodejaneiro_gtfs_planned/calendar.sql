SELECT 
SAFE_CAST(service_id AS STRING) service_id,
SAFE_CAST(monday AS INT64) monday,
SAFE_CAST(tuesday AS INT64) tuesday,
SAFE_CAST(wednesday AS INT64) wednesday,
SAFE_CAST(thursday AS INT64) thursday,
SAFE_CAST(friday AS INT64) friday,
SAFE_CAST(saturday AS INT64) saturday,
SAFE_CAST(sunday AS INT64) sunday,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", start_date, "America/Sao_Paulo")) AS DATETIME) start_date,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", end_date, "America/Sao_Paulo")) AS DATETIME) end_date,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.calendar as t