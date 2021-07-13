with logs as (
SELECT 
    data,
    extract(hour from timestamp_captura) as hora,
    COUNT(distinct timestamp_captura) as capturas,
    COUNT(CASE WHEN sucesso = false THEN 1 END) falhas,
    COUNT(CASE WHEN erro like "%Read timed out.%" THEN 1 END) timeouts
FROM `rj-smtr-dev.br_rj_riodejaneiro_onibus_gps.registros_logs` 
group by data, hora
)

SELECT
        data,
        hora,
        capturas,
        falhas,
        timeouts,
        CASE
            WHEN hora between 5 and 8 THEN "manhã"
            WHEN hora between 16 and 19 THEN "noite"
        END as pico,
        CASE
            WHEN capturas < 45 THEN false
            WHEN capturas > 45 AND falhas/capturas > 0.1 THEN true
            ELSE false
        END as hora_multavel
FROM logs
order by data, hora