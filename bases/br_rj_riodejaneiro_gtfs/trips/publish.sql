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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.trips AS
SELECT 
SAFE_CAST(route_id AS STRING) route_id,
SAFE_CAST(service_id AS STRING) service_id,
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(trip_headsign AS STRING) trip_headsign,
SAFE_CAST(direction_id AS INT64) direction_id,
SAFE_CAST(shape_id AS STRING) shape_id,
SAFE_CAST(trip_short_name AS STRING) trip_short_name,
SAFE_CAST(block_id AS STRING) block_id,
SAFE_CAST(wheelchair_accessible AS INT64) wheelchair_accessible,
SAFE_CAST(bikes_allowed AS INT64) bikes_allowed,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.trips as t