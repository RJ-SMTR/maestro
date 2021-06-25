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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_brt_gtfs_gps.realized_trips AS
SELECT 
SAFE_CAST(vehicle_id AS STRING) vehicle_id,
SAFE_CAST(route_id AS STRING) route_id,
SAFE_CAST(direction_id AS STRING) direction_id,
SAFE_CAST(service_id AS STRING) service_id,
SAFE_CAST(trip_id AS STRING) trip_id,
SAFE_CAST(departure_datetime AS STRING) departure_datetime,
SAFE_CAST(arrival_datetime AS STRING) arrival_datetime
from rj-smtr-dev.br_rj_riodejaneiro_brt_gtfs_gps_staging.realized_trips as t