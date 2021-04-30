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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.calendar AS
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
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.calendar as t