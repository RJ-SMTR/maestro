WITH garagem_polygon AS (
  SELECT 
    ST_GEOGFROMTEXT(WKT, make_valid => true) AS poly
  FROM `rj-smtr.br_rj_riodejaneiro_geo.garagens_polygon` 
),
box AS (
  SELECT
    *
  FROM `rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa`
),
gps AS (
  SELECT 
    *,
    ST_GEOGPOINT(longitude, latitude) ponto
  FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros`
  WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < 2
  AND data between DATE_SUB(CURRENT_DATE(), INTERVAL 8 DAY) and CURRENT_DATE()
)
SELECT
  ordem, latitude, longitude, timestamp_gps, velocidade, linha, timestamp_captura, data, hora,
  extract(time from gps.timestamp_captura) as hora_completa,
  ST_INTERSECTS(ponto, (SELECT poly FROM garagem_polygon)) fora_garagem
FROM gps
WHERE ST_INTERSECTSBOX(ponto, (SELECT min_longitude FROM box), (SELECT min_latitude FROM box), (SELECT max_longitude FROM box), (SELECT max_latitude FROM box))
