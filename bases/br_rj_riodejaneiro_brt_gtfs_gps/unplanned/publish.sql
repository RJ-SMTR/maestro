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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_brt_gtfs_gps.unplanned AS
SELECT 
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(n_registros AS INT64) n_registros,
SAFE_CAST(dia AS DATE) dia
from rj-smtr-staging.br_rj_riodejaneiro_brt_gtfs_gps_staging.unplanned as t