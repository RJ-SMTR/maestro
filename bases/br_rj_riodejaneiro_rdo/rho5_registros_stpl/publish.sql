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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_rdo.rho5_registros_stpl AS
SELECT 
SAFE_CAST(operadora AS STRING) operadora,
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(PARSE_DATETIME("%Y-%m-%d", data_transacao) AS DATETIME) data_transacao,
SAFE_CAST(hora_transacao AS INT64) hora_transacao,
SAFE_CAST(total_gratuidades AS INT64) total_gratuidades,
SAFE_CAST(total_pagantes AS INT64) total_pagantes,
SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(dia AS INT64) dia
from rj-smtr-staging.br_rj_riodejaneiro_rdo_staging.rho5_registros_stpl as t