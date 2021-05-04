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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_gtfs.routes AS
SELECT 
SAFE_CAST(route_id AS STRING) route_id,
SAFE_CAST(agency_id AS STRING) agency_id,
SAFE_CAST(route_short_name AS STRING) route_short_name,
SAFE_CAST(route_long_name AS STRING) route_long_name,
SAFE_CAST(route_type AS INT64) route_type,
SAFE_CAST(route_color AS STRING) route_color,
SAFE_CAST(route_text_color AS STRING) route_text_color,
SAFE_CAST(route_desc AS STRING) route_desc,
SAFE_CAST(route_url AS STRING) route_url,
SAFE_CAST(route_sort_order AS INT64) route_sort_order,
SAFE_CAST(continuous_pickup AS INT64) continuous_pickup,
SAFE_CAST(continuous_drop_off AS INT64) continuous_drop_off,
SAFE_CAST(DATETIME(PARSE_TIMESTAMP("%Y%m%d", CAST(gtfs_version_date AS STRING), "America/Sao_Paulo")) AS DATETIME) gtfs_version_date
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_staging.routes as t