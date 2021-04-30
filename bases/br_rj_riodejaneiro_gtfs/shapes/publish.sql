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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_gtfs.shapes AS
SELECT 
SAFE_CAST(shape_id AS STRING) shape_id,
SAFE_CAST(shape_pt_lat AS STRING) shape_pt_lat,
SAFE_CAST(shape_pt_lon AS STRING) shape_pt_lon,
SAFE_CAST(shape_pt_sequence AS STRING) shape_pt_sequence,
SAFE_CAST(shape_dist_traveled AS STRING) shape_dist_traveled,
SAFE_CAST(gtfs_version_date AS STRING) gtfs_version_date
from rj-smtr-dev.br_rj_riodejaneiro_gtfs_staging.shapes as t