with satisfacao_estacao as (
SELECT
  t3.id_responsavel,
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
  t3.id_responsavel,
  t3.nome_estacao
order by t3.id_responsavel,nome_estacao)
select
  t1.id_responsavel,
  t2.nome_exibicao_responsavel,
  t2.email_responsavel,
  t2.telefone_responsavel,
  nome_estacao,
  seriedade,
  split(seriedade, '(')[safe_offset(0)] seriedade_simples,
  ordem_seriedade
from satisfacao_estacao t1
join `rj-smtr.brt_manutencao.responsaveis` t2
on t1.id_responsavel = t2.id_responsavel