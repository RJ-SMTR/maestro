WITH
  satisfacao_problema AS (
  SELECT
    t3.nome_problema,
    t3.id_responsavel,
    t3.categoria_problema,
    SUM(CASE
        WHEN t4.id_seriedade = 'satisfatorio' THEN 1
      ELSE
      0
    END
      ) / COUNT(*) satisfacao
  FROM
    `rj-smtr.brt_manutencao.questionario_recentes` t3
  JOIN
    `rj-smtr.brt_manutencao.seriedade` t4
  ON
    t3.seriedade = t4.nome_seriedade
  GROUP BY
    t3.nome_problema,
    t3.id_responsavel,
    categoria_problema
  ORDER BY
    satisfacao DESC)
SELECT 
  nome_problema, categoria_problema, t1.id_responsavel, t1.nome_exibicao_responsavel, satisfacao * 100 satisfacao
FROM satisfacao_problema t0
JOIN `rj-smtr.brt_manutencao.responsaveis` t1
ON t0.id_responsavel = t1.id_responsavel