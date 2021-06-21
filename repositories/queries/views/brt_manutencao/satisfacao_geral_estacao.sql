with satisfacao_estacao as (
SELECT
  t3.corredor_estacao,
  t3.nome_estacao,
  ARRAY_AGG(nome_seriedade order by ordem_seriedade desc limit 1)[offset(0)] seriedade,
  MAX(ordem_seriedade) ordem_seriedade
FROM
  `rj-smtr.brt_manutencao.questionario_recentes` t3
JOIN
  `rj-smtr.brt_manutencao.seriedade` t4
ON
  t3.seriedade = t4.nome_seriedade
GROUP BY
  t3.corredor_estacao,
  t3.nome_estacao)
select
  corredor_estacao,
  nome_estacao,
  seriedade,
  split(seriedade, '(')[safe_offset(0)] seriedade_simples,
  ordem_seriedade
from satisfacao_estacao