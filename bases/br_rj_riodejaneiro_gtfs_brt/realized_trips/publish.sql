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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_gtfs_brt.realized_trips AS
SELECT 
SAFE_CAST(vehicle_id AS STRING) vehicle_id,
SAFE_CAST(departure_datetime AS DATETIME) departure_datetime,
SAFE_CAST(arrival_datetime AS DATETIME) arrival_datetime,
SAFE_CAST(departure_id AS STRING) departure_id,
SAFE_CAST(arrival_id AS STRING) arrival_id,
SAFE_CAST(distance AS FLOAT64) distance,
SAFE_CAST(elapsed_time AS FLOAT64) elapsed_time,
SAFE_CAST(average_speed AS FLOAT64) average_speed,
SAFE_CAST(trajectory_type AS STRING) trajectory_type,
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(trip_short_name AS STRING) trip_short_name
from rj-smtr-staging.br_rj_riodejaneiro_gtfs_brt_staging.realized_trips as t