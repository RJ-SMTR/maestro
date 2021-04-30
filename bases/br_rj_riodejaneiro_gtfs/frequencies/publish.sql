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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.frequencies AS
SELECT 
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(start_time AS STRING) start_time,
SAFE_CAST(end_time AS STRING) end_time,
SAFE_CAST(headway_secs AS STRING) headway_secs,
SAFE_CAST(exact_times AS STRING) exact_times,
SAFE_CAST(gtfs_version_date AS STRING) gtfs_version_date
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.frequencies as t