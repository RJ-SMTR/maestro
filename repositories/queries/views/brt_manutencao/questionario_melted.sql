SELECT *
FROM (
  SELECT ts as timestamp, estacao_brt as estacoes_brt, 
    REGEXP_REPLACE(SPLIT(pair, ':')[SAFE_OFFSET(0)], r'^"|"$', '') id_problema, 
    SPLIT(REGEXP_REPLACE(SPLIT(pair, ':')[SAFE_OFFSET(1)], r'^"|"$', ''), ' ')[SAFE_OFFSET(0)] seriedade 
  FROM `rj-smtr.brt_manutencao.questionario` t, 
  UNNEST(SPLIT(REGEXP_REPLACE(REGEXP_REPLACE(to_json_string(t), r'{|}', ''), r'","', '|'), '|')) pair
)
WHERE NOT LOWER(id_problema) IN (
  'ts', 'estacao_brt', 'email',
  'comentarios', 'imagens', 'numero', 'nome_pesquisador', 'imagens_comentarios')
AND lower(id_problema) not like 'foto_%'