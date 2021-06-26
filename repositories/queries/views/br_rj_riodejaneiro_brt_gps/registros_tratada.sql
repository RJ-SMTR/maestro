WITH box AS (
  SELECT
    *
  FROM `rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa`
)
SELECT *
FROM `rj-smtr.br_rj_riodejaneiro_brt_gps.registros` 
WHERE DATETIME_DIFF(timestamp_captura, timestamp_gps, MINUTE) < 2
AND longitude BETWEEN (SELECT min_longitude FROM box) AND (SELECT max_longitude FROM box)
AND latitude BETWEEN (SELECT min_latitude FROM box) AND (SELECT max_latitude FROM box)
