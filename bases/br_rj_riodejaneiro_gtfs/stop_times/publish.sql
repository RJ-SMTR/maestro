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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.stop_times AS
SELECT 
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(arrival_time AS STRING) arrival_time,
SAFE_CAST(departure_time AS STRING) departure_time,
SAFE_CAST(stop_id AS STRING) stop_id,
SAFE_CAST(stop_sequence AS STRING) stop_sequence,
SAFE_CAST(stop_headsign AS STRING) stop_headsign,
SAFE_CAST(pickup_type AS STRING) pickup_type,
SAFE_CAST(drop_off_type AS STRING) drop_off_type,
SAFE_CAST(continuous_pickup AS STRING) continuous_pickup,
SAFE_CAST(continuous_drop_off AS STRING) continuous_drop_off,
SAFE_CAST(shape_dist_traveled AS STRING) shape_dist_traveled,
SAFE_CAST(timepoint AS STRING) timepoint,
SAFE_CAST(gtfs_version_date AS STRING) gtfs_version_date
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.stop_times as t