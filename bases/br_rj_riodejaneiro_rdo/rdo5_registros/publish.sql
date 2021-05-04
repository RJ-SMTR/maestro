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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_rdo.rdo5_registros AS
SELECT 
SAFE_CAST(operadora AS STRING) operadora,
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(PARSE_DATETIME("%Y-%m-%d", data_transacao) AS DATETIME) data_transacao,
SAFE_CAST(tarifa_valor AS FLOAT64) tarifa_valor,
SAFE_CAST(gratuidade_idoso AS INT64) gratuidade_idoso,
SAFE_CAST(gratuidade_especial AS INT64) gratuidade_especial,
SAFE_CAST(gratuidade_estudante_federal AS INT64) gratuidade_estudante_federal,
SAFE_CAST(gratuidade_estudante_estadual AS INT64) gratuidade_estudante_estadual,
SAFE_CAST(gratuidade_estudante_municipal AS INT64) gratuidade_estudante_municipal,
SAFE_CAST(universitario AS INT64) universitario,
SAFE_CAST(gratuito_rodoviario AS INT64) gratuito_rodoviario,
SAFE_CAST(buc_1a_perna AS INT64) buc_1a_perna,
SAFE_CAST(buc_2a_perna AS INT64) buc_2a_perna,
SAFE_CAST(buc_receita AS FLOAT64) buc_receita,
SAFE_CAST(buc_supervia_1a_perna AS INT64) buc_supervia_1a_perna,
SAFE_CAST(buc_supervia_2a_perna AS INT64) buc_supervia_2a_perna,
SAFE_CAST(buc_supervia_receita AS FLOAT64) buc_supervia_receita,
SAFE_CAST(buc_van_1a_perna AS INT64) buc_van_1a_perna,
SAFE_CAST(buc_van_2a_perna AS INT64) buc_van_2a_perna,
SAFE_CAST(buc_van_receita AS FLOAT64) buc_van_receita,
SAFE_CAST(buc_vlt_1a_perna AS INT64) buc_vlt_1a_perna,
SAFE_CAST(buc_vlt_2a_perna AS INT64) buc_vlt_2a_perna,
SAFE_CAST(buc_vlt_receita AS FLOAT64) buc_vlt_receita,
SAFE_CAST(buc_brt_1a_perna AS INT64) buc_brt_1a_perna,
SAFE_CAST(buc_brt_2a_perna AS INT64) buc_brt_2a_perna,
SAFE_CAST(buc_brt_3a_perna AS INT64) buc_brt_3a_perna,
SAFE_CAST(buc_brt_receita AS FLOAT64) buc_brt_receita,
SAFE_CAST(buc_inter_1a_perna AS INT64) buc_inter_1a_perna,
SAFE_CAST(buc_inter_2a_perna AS INT64) buc_inter_2a_perna,
SAFE_CAST(buc_inter_receita AS FLOAT64) buc_inter_receita,
SAFE_CAST(buc_barcas_1a_perna AS INT64) buc_barcas_1a_perna,
SAFE_CAST(buc_barcas_2a_perna AS INT64) buc_barcas_2a_perna,
SAFE_CAST(buc_barcas_receita AS FLOAT64) buc_barcas_receita,
SAFE_CAST(buc_metro_1a_perna AS INT64) buc_metro_1a_perna,
SAFE_CAST(buc_metro_2a_perna AS INT64) buc_metro_2a_perna,
SAFE_CAST(buc_metro_receita AS FLOAT64) buc_metro_receita,
SAFE_CAST(cartao AS INT64) cartao,
SAFE_CAST(receita_cartao AS FLOAT64) receita_cartao,
SAFE_CAST(especie_passageiro_transportado AS INT64) especie_passageiro_transportado,
SAFE_CAST(especie_receita AS FLOAT64) especie_receita,
SAFE_CAST(registro_processado AS STRING) registro_processado,
SAFE_CAST(PARSE_DATETIME("%Y%m%d", data_processamento) AS DATETIME) data_processamento,
SAFE_CAST(linha_rcti AS STRING) linha_rcti,
SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes,
SAFE_CAST(dia AS INT64) dia
from rj-smtr-staging.br_rj_riodejaneiro_rdo_staging.rdo5_registros as t