WITH paradas as (
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_estacao nome_parada, 'estacao' tipo_parada
  from `rj-smtr.br_rj_riodejaneiro_transporte.estacoes_e_terminais_brt` t1
  union all
  select
    ST_GEOGPOINT(longitude, latitude) ponto_parada, nome_empresa nome_parada, 'garagem' tipo_parada
  from `rj-smtr.br_rj_riodejaneiro_transporte.garagens` t2
  where ativa = 1),
onibus_parados AS (
  select
    *, ST_GEOGPOINT(longitude, latitude) ponto_carro
  from `rj-smtr.br_rj_riodejaneiro_brt_gps.aux_registros_velocidade_carro` 
  ),
distancia AS (
  SELECT 
    timestamp_captura, velocidade, placa, longitude, latitude, nome_parada, tipo_parada,
    ST_DISTANCE(ponto_carro, ponto_parada) distancia_parada, 
    ROW_NUMBER() OVER (PARTITION BY timestamp_captura, placa ORDER BY ST_DISTANCE(ponto_carro, ponto_parada)) nrow
  FROM paradas p
  join onibus_parados o
  on 1=1
  )
SELECT
  * except(nrow),
  case
    when velocidade < 3 then 'parado'
    else 'andando'
  end status_movimento,
  case
    when distancia_parada < 1000 then tipo_parada
    else 'nao_identificado'
  end status_tipo_parada
FROM distancia
WHERE nrow = 1