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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_gtfs.agency AS
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