with comparativo_frota as (
SELECT * 
FROM `rj-smtr-dev.projeto_multa_automatica.comparativo_frota_flag`
where data = DATE("2021-07-10")
),
horas_multaveis as (
SELECT 
        data,
        pico,
        COUNT(CASE WHEN hora_multavel = true THEN 1 END) horas_multaveis
FROM `rj-smtr-dev.projeto_multa_automatica.horas_multaveis` 
where pico is not null and data = DATE("2021-07-10")
group by data, pico
)

SELECT * 
from comparativo_frota 
inner join horas_multaveis
on comparativo_frota.data = horas_multaveis.data AND comparativo_frota.pico = horas_multaveis.pico