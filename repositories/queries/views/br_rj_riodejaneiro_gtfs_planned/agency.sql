SELECT 
SAFE_CAST(agency_id AS INT64) agency_id,
SAFE_CAST(agency_name AS STRING) agency_name,
SAFE_CAST(agency_url AS STRING) agency_url,
SAFE_CAST(agency_timezone AS STRING) agency_timezone,
SAFE_CAST(agency_lang AS STRING) agency_lang,
SAFE_CAST(agency_phone AS STRING) agency_phone,
SAFE_CAST(agency_fare_url AS STRING) agency_fare_url,
SAFE_CAST(agency_email AS STRING) agency_email,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.agency as t