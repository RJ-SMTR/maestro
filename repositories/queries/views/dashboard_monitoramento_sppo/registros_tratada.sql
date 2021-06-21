WITH garagens AS (
  SELECT 
    ST_GEOGPOINT(longitude, latitude) ponto_garagem
  FROM `rj-smtr.br_rj_riodejaneiro_transporte.garagens`
),
box AS (
  SELECT
    *
  FROM `rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa`
),
gps AS (
  SELECT *
  FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros` 
  WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < 2
  AND longitude BETWEEN (SELECT min_longitude FROM box) AND (SELECT max_longitude FROM box)
  AND latitude BETWEEN (SELECT min_latitude FROM box) AND (SELECT max_latitude FROM box)
  AND data between DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) and CURRENT_DATE()
),
distancia AS (
  SELECT 
    longitude, latitude,
    MIN(ST_DISTANCE(ST_GEOGPOINT(longitude, latitude), ponto_garagem))
    distancia_da_garagem_metros
  FROM garagens 
  join gps
  on 1=1
  GROUP BY longitude, latitude
)
SELECT
  gps.*, 
  extract(time from gps.timestamp_captura) as hora_completa,
  distancia.distancia_da_garagem_metros
FROM gps
JOIN distancia 
ON gps.latitude = distancia.latitude
AND gps.longitude = distancia.longitude