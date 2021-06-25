WITH gps AS (
  SELECT *, 
    REGEXP_REPLACE(SPLIT(trajeto, ' ')[SAFE_OFFSET(0)], '[^a-zA-Z0-9]', '') linha_trajeto
  FROM `rj-smtr.dashboard_monitoramento_brt.registros_filtrada` 
)
SELECT 
  t.*,
  extract(time from t.timestamp_gps) as hora_completa,
  linha_trajeto = linha flag_linha_similar_trajeto,
  t2.velocidade as velocidade_estimada_10_min,
  t2.nome_parada,
  t2.tipo_parada,
  t2.distancia_parada,
  t2.status_movimento,
  t2.status_tipo_parada
FROM gps t
JOIN `rj-smtr.dashboard_monitoramento_brt.velocidade_status` t2
ON t.timestamp_captura = t2.timestamp_captura
AND t.placa = t2.placa