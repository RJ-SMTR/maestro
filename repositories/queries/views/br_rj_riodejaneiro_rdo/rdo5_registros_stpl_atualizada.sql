WITH
  updated_rdo AS (
  SELECT
    MAX(data_processamento) AS data_processamento,
    data_transacao,
    linha,
    operadora,
  FROM
    `rj-smtr.br_rj_riodejaneiro_rdo.rdo5_registros_stpl`
  GROUP BY
    data_transacao,
    linha,
    operadora
     )
SELECT
  all_rdo.*
FROM
  updated_rdo
JOIN
  `rj-smtr.br_rj_riodejaneiro_rdo.rdo5_registros_stpl` all_rdo
ON
  updated_rdo.data_processamento = all_rdo.data_processamento
  AND updated_rdo.data_transacao = all_rdo.data_transacao
  AND updated_rdo.linha = all_rdo.linha
  AND updated_rdo.operadora = all_rdo.operadora;