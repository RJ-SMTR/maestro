/*

Query para publicar a tabela.

Esse é o lugar para:
    - modificar nomes, ordem e tipos de colunas
    - dar join com outras tabelas
    - criar colunas extras (e.g. logs, proporções, etc.)

Qualquer coluna definida aqui deve também existir em `table_config.yaml`.

# Além disso, sinta-se à vontade para alterar alguns nomes obscuros
# para algo um pouco mais explícito.

TIPOS:
    - Para modificar tipos de colunas, basta substituir STRING por outro tipo válido.
    - Exemplo: `SAFE_CAST(column_name AS NUMERIC) column_name`
    - Mais detalhes: https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types

*/

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.stops AS
SELECT 
SAFE_CAST(stop_id AS STRING) stop_id,
SAFE_CAST(stop_name AS STRING) stop_name,
SAFE_CAST(stop_desc AS STRING) stop_desc,
SAFE_CAST(stop_lat AS FLOAT64) stop_lat,
SAFE_CAST(stop_lon AS FLOAT64) stop_lon,
SAFE_CAST(location_type AS INT64) location_type,
SAFE_CAST(parent_station AS STRING) parent_station,
SAFE_CAST(corridor AS STRING) corridor,
SAFE_CAST(active AS INT64) active,
SAFE_CAST(stop_code AS STRING) stop_code,
SAFE_CAST(tts_stop_name AS STRING) tts_stop_name,
SAFE_CAST(zone_id AS STRING) zone_id,
SAFE_CAST(stop_url AS STRING) stop_url,
SAFE_CAST(stop_timezone AS STRING) stop_timezone,
SAFE_CAST(wheelchair_boarding AS INT64) wheelchair_boarding,
SAFE_CAST(level_id AS STRING) level_id,
SAFE_CAST(platform_code AS STRING) platform_code,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.stops as t