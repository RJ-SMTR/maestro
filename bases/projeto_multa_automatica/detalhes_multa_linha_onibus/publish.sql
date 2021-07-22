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

CREATE VIEW rj-smtr.projeto_multa_automatica.detalhes_multa_linha_onibus AS
SELECT 
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(pico AS STRING) pico,
SAFE_CAST(faixa_horaria AS STRING) faixa_horaria,
SAFE_CAST(frota_aferida AS INT64) frota_aferida,
SAFE_CAST(frota_determinada AS INT64) frota_determinada,
SAFE_CAST(porcentagem_frota AS FLOAT64) porcentagem_frota,
SAFE_CAST(tipo_multa AS STRING) tipo_multa,
SAFE_CAST(artigo_multa AS STRING) artigo_multa,
SAFE_CAST(data AS DATE) data
from rj-smtr-staging.projeto_multa_automatica_staging.detalhes_multa_linha_onibus as t