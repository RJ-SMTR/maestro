SELECT 
SAFE_CAST(shape_id AS STRING) shape_id,
SAFE_CAST(shape_pt_lat AS FLOAT64) shape_pt_lat,
SAFE_CAST(shape_pt_lon AS FLOAT64) shape_pt_lon,
SAFE_CAST(shape_pt_sequence AS INT64) shape_pt_sequence,
SAFE_CAST(shape_dist_traveled AS FLOAT64) shape_dist_traveled,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.shapes as t