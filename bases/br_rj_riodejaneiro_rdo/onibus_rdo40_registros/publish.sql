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

CREATE VIEW rj-smtr.br_rj_riodejaneiro_rdo.onibus_rdo40_registros AS
SELECT 
SAFE_CAST(operadora AS STRING) operadora,
SAFE_CAST(linha AS STRING) linha,
SAFE_CAST(servico_tipo AS STRING) servico_tipo,
SAFE_CAST(servico_termo AS STRING) servico_termo,
SAFE_CAST(tipo_veiculo AS STRING) tipo_veiculo,
SAFE_CAST(data_ano AS INT64) data_ano,
SAFE_CAST(data_mes AS INT64) data_mes,
SAFE_CAST(data_dia AS INT64) data_dia,
SAFE_CAST(tarifa_codigo AS STRING) tarifa_codigo,
SAFE_CAST(tarifa_valor AS FLOAT64) tarifa_valor,
SAFE_CAST(frota_determinada AS INT64) frota_determinada,
SAFE_CAST(frota_licenciada AS INT64) frota_licenciada,
SAFE_CAST(frota_operante AS INT64) frota_operante,
SAFE_CAST(viagem_realizada AS INT64) viagem_realizada,
SAFE_CAST(km AS FLOAT64) km,
SAFE_CAST(gratuidade_idoso AS INT64) gratuidade_idoso,
SAFE_CAST(gratuidade_especial AS INT64) gratuidade_especial,
SAFE_CAST(gratuidade_estudante_federal AS INT64) gratuidade_estudante_federal,
SAFE_CAST(gratuidade_estudante_estadual AS INT64) gratuidade_estudante_estadual,
SAFE_CAST(gratuidade_estudante_municipal AS INT64) gratuidade_estudante_municipal,
SAFE_CAST(gratuidade_rodoviario AS INT64) gratuidade_rodoviario,
SAFE_CAST(universitario AS INT64) universitario,
SAFE_CAST(gratuidade_total AS INT64) gratuidade_total,
SAFE_CAST(buc_1a_perna AS INT64) buc_1a_perna,
SAFE_CAST(buc_2a_perna AS INT64) buc_2a_perna,
SAFE_CAST(buc_receita AS FLOAT64) buc_receita,
SAFE_CAST(buc_supervia_1a_perna AS INT64) buc_supervia_1a_perna,
SAFE_CAST(buc_supervia_2a_perna AS INT64) buc_supervia_2a_perna,
SAFE_CAST(buc_supervia_receita AS FLOAT64) buc_supervia_receita,
SAFE_CAST(perna_unica_e_outros_transportado AS INT64) perna_unica_e_outros_transportado,
SAFE_CAST(perna_unica_e_outros_receita AS FLOAT64) perna_unica_e_outros_receita,
SAFE_CAST(especie_passageiro_transportado AS INT64) especie_passageiro_transportado,
SAFE_CAST(especie_receita AS FLOAT64) especie_receita,
SAFE_CAST(total_passageiro_transportado AS INT64) total_passageiro_transportado,
SAFE_CAST(total_receita AS FLOAT64) total_receita,
SAFE_CAST(tipo_informacao AS STRING) tipo_informacao,
SAFE_CAST(DATETIME(TIMESTAMP(timestamp_captura), "America/Sao_Paulo") AS DATETIME) timestamp_captura,
SAFE_CAST(ano AS INT64) ano,
SAFE_CAST(mes AS INT64) mes
from rj-smtr-staging.br_rj_riodejaneiro_rdo_staging.onibus_rdo40_registros as t
