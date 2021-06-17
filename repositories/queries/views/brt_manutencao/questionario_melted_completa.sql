WITH
  todos_problemas_estacoes AS (
  SELECT
    t1.codigo_estacao as id_estacao,
    t1.nome_estacao,
    t1.ativa as ativa_estacao,
    t1.corredor as corredor_estacao,
    t2.id_problema,
    t2.nome_problema,
    t2.categoria as categoria_problema,
    t2.id_responsavel
  FROM
    `rj-smtr.brt_manutencao.estacoes` t1
  JOIN
    `rj-smtr.brt_manutencao.problemas` t2
  ON
    TRUE 
  WHERE t1.ativa = '1')
SELECT
  DATE(timestamp) dt,
  FLOOR(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, DAY)) idade_resposta_dia,
  FLOOR(TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), timestamp, DAY) / 7) idade_resposta_semana,
  coalesce( t3.estacoes_brt, t4.nome_estacao) nome_estacao,  
  ativa_estacao,
  corredor_estacao,
  categoria_problema,
  nome_problema,
  coalesce(t3.id_problema, t4.id_problema) id_problema,
  id_responsavel,
  coalesce(split(t3.seriedade, ' ')[SAFE_OFFSET(0)], 'Sem Avaliação') seriedade
FROM
  `rj-smtr.brt_manutencao.questionario_melted` t3
FULL JOIN
  todos_problemas_estacoes t4
ON
  t3.estacoes_brt = t4.nome_estacao
  AND t3.id_problema = t4.id_problema