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

CREATE VIEW rj-smtr-dev.br_rj_riodejaneiro_rdo.rdo5_registros AS
SELECT 
SAFE_CAST(operadora AS STRING) operadora,
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(data_transacao AS STRING) data_transacao,
SAFE_CAST(tarifa_valor AS STRING) tarifa_valor,
SAFE_CAST(gratuidade_idoso AS STRING) gratuidade_idoso,
SAFE_CAST(gratuidade_especial AS STRING) gratuidade_especial,
SAFE_CAST(gratuidade_estudante_federal AS STRING) gratuidade_estudante_federal,
SAFE_CAST(gratuidade_estudante_estadual AS STRING) gratuidade_estudante_estadual,
SAFE_CAST(gratuidade_estudante_municipal AS STRING) gratuidade_estudante_municipal,
SAFE_CAST(universitario AS STRING) universitario,
SAFE_CAST(gratuito_rodoviario AS STRING) gratuito_rodoviario,
SAFE_CAST(buc_1a_perna AS STRING) buc_1a_perna,
SAFE_CAST(buc_2a_perna AS STRING) buc_2a_perna,
SAFE_CAST(buc_receita AS STRING) buc_receita,
SAFE_CAST(buc_supervia_1a_perna AS STRING) buc_supervia_1a_perna,
SAFE_CAST(buc_supervia_2a_perna AS STRING) buc_supervia_2a_perna,
SAFE_CAST(buc_supervia_receita AS STRING) buc_supervia_receita,
SAFE_CAST(buc_van_1a_perna AS STRING) buc_van_1a_perna,
SAFE_CAST(buc_van_2a_perna AS STRING) buc_van_2a_perna,
SAFE_CAST(buc_van_receita AS STRING) buc_van_receita,
SAFE_CAST(buc_vlt_1a_perna AS STRING) buc_vlt_1a_perna,
SAFE_CAST(buc_vlt_2a_perna AS STRING) buc_vlt_2a_perna,
SAFE_CAST(buc_vlt_receita AS STRING) buc_vlt_receita,
SAFE_CAST(buc_brt_1a_perna AS STRING) buc_brt_1a_perna,
SAFE_CAST(buc_brt_2a_perna AS STRING) buc_brt_2a_perna,
SAFE_CAST(buc_brt_3a_perna AS STRING) buc_brt_3a_perna,
SAFE_CAST(buc_brt_receita AS STRING) buc_brt_receita,
SAFE_CAST(buc_inter_1a_perna AS STRING) buc_inter_1a_perna,
SAFE_CAST(buc_inter_2a_perna AS STRING) buc_inter_2a_perna,
SAFE_CAST(buc_inter_receita AS STRING) buc_inter_receita,
SAFE_CAST(buc_barcas_1a_perna AS STRING) buc_barcas_1a_perna,
SAFE_CAST(buc_barcas_2a_perna AS STRING) buc_barcas_2a_perna,
SAFE_CAST(buc_barcas_receita AS STRING) buc_barcas_receita,
SAFE_CAST(buc_metro_1a_perna AS STRING) buc_metro_1a_perna,
SAFE_CAST(buc_metro_2a_perna AS STRING) buc_metro_2a_perna,
SAFE_CAST(buc_metro_receita AS STRING) buc_metro_receita,
SAFE_CAST(cartao AS STRING) cartao,
SAFE_CAST(receita_cartao AS STRING) receita_cartao,
SAFE_CAST(especie_passageiro_transportado AS STRING) especie_passageiro_transportado,
SAFE_CAST(especie_receita AS STRING) especie_receita,
SAFE_CAST(registro_processado AS STRING) registro_processado,
SAFE_CAST(data_processamento AS STRING) data_processamento,
SAFE_CAST(linha_rcti AS STRING) linha_rcti,
SAFE_CAST(codigo AS STRING) codigo,
SAFE_CAST(timestamp_captura AS STRING) timestamp_captura,
SAFE_CAST(ano AS STRING) ano,
SAFE_CAST(mes AS STRING) mes,
SAFE_CAST(dia AS STRING) dia
from rj-smtr-dev.br_rj_riodejaneiro_rdo_staging.rdo5_registros as t