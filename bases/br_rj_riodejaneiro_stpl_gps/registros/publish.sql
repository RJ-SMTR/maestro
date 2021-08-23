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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_stpl_gps.registros AS
SELECT 
SAFE_CAST(codigo AS STRING) codigo,
SAFE_CAST(placa AS STRING) placa,
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(latitude AS STRING) latitude,
SAFE_CAST(longitude AS STRING) longitude,
SAFE_CAST(dataHora AS STRING) dataHora,
SAFE_CAST(velocidade AS STRING) velocidade,
SAFE_CAST(id_migracao_trajeto AS STRING) id_migracao_trajeto,
SAFE_CAST(sentido AS STRING) sentido,
SAFE_CAST(trajeto AS STRING) trajeto,
SAFE_CAST(timestamp_captura AS STRING) timestamp_captura,
SAFE_CAST(data AS STRING) data,
SAFE_CAST(hora AS STRING) hora
from rj-smtr-staging.br_rj_riodejaneiro_stpl_gps_staging.registros as t