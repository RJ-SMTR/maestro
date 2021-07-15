with registros as (
SELECT  data,
        hora,
        linha,
        n_veiculos 
FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_agg_data_hora_linha` 
WHERE data = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
),
horas_multaveis as (
SELECT  data,
        hora,
        pico,
        hora_multavel 
FROM `rj-smtr-dev.projeto_multa_automatica.horas_multaveis_onibus`
WHERE data = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
AND hora_multavel = "Multavel por frota" 
)

SELECT  registros.data as data,
        registros.linha as linha,
        ROUND(AVG(n_veiculos)) as frota_realizada,
        horas_multaveis.pico as pico,
        COUNT(hora_multavel) as horas_multaveis
FROM registros
INNER JOIN horas_multaveis 
ON registros.data = horas_multaveis.data
    and registros.hora = horas_multaveis.hora 
WHERE pico is not null
GROUP BY data, linha, pico
order by linha