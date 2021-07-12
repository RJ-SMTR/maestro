WITH wrows AS (
  SELECT ST_GEOGPOINT(longitude, latitude) point, timestamp_captura, timestamp_gps, latitude, longitude, placa,
        ROW_NUMBER() OVER (PARTITION BY placa ORDER BY timestamp_captura) n_row
  from `rj-smtr.br_rj_riodejaneiro_brt_gps.registros_tratada_8_dias`),  
distances AS (
  SELECT
    t1.timestamp_captura ts1, 
    t2.timestamp_captura ts2, 
    t1.latitude, t1.longitude, t1.placa,
    DATETIME_DIFF(t2.timestamp_captura, t1.timestamp_captura, SECOND) / 60 minutos,
    ST_DISTANCE(t1.point, t2.point) distancia
  FROM wrows t1
  JOIN wrows t2
  ON t1.n_row = t2.n_row -1
  AND t1.placa = t2.placa
  ),
times AS (
  SELECT ts
  FROM (
    SELECT
        CAST(MIN(data) AS TIMESTAMP) min_date, TIMESTAMP_ADD(CAST(MAX(data) AS TIMESTAMP), INTERVAL 1 DAY) max_date
    FROM `rj-smtr.br_rj_riodejaneiro_brt_gps.registros_tratada_8_dias`) t 
  JOIN UNNEST(GENERATE_TIMESTAMP_ARRAY(t.min_date, t.max_date, INTERVAL 10 MINUTE)) ts
),
speed AS (
  SELECT
    ts,
    d.*
  FROM times
  JOIN distances d
  ON NOT(
      ts2 < DATETIME(ts) OR 
      ts1 > DATETIME_ADD(DATETIME(ts), INTERVAL 10 MINUTE))
 )
SELECT
  ts2 as timestamp_captura, t1.placa, latitude, longitude, AVG(t1.velocidade) velocidade
FROM speed
JOIN (SELECT ts, placa, avg(SAFE_DIVIDE(distancia, minutos) * 6/100) velocidade 
      FROM speed 
      GROUP BY ts, placa) t1
ON t1.ts = speed.ts 
AND t1.placa = speed.placa
GROUP BY ts2, placa, latitude, longitude