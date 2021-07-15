with 
realizada as (
SELECT  data,
        linha,
        pico,
        frota_realizada,
        horas_multaveis
FROM `rj-smtr-dev.projeto_multa_automatica.frota_realizada_filtrada_1_dia` 
),
planejada as (
SELECT  data,
        linha,
        pico,
        frota_planejada
FROM `rj-smtr-dev.projeto_multa_automatica.frota_planejada_1_dia`
WHERE   data = DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 DAY)
        and pico is not null
),
horas as (
SELECT  data,
        pico,
        COUNT(CASE WHEN hora_multavel = 'erro smtr' THEN 1 END) as horas_desconsideradas,
        COUNT(CASE WHEN hora_multavel = 'multavel api' THEN 1 END) as horas_instabilidade,

FROM `rj-smtr-dev.projeto_multa_automatica.horas_multaveis_onibus` 
WHERE   data = DATE_SUB(CURRENT_DATE("America/Sao_Paulo"), INTERVAL 1 DAY)
        and pico is not null
GROUP BY data, pico
)

SELECT  realizada.data as data,
        realizada.linha as linha,
        realizada.pico as pico,
        horas_multaveis,
        horas_desconsideradas,
        horas_instabilidade,
        frota_realizada,
        frota_planejada,
        ROUND(frota_realizada/frota_planejada, 2) as porcentagem_operacao,
        CASE 
                WHEN frota_realizada/frota_planejada < 0.8 then true
                ELSE false
                END as flag_operacao
FROM realizada
INNER JOIN planejada
ON      realizada.data = planejada.data
        and realizada.linha = planejada.linha
        and realizada.pico = planejada.pico
INNER JOIN horas
ON realizada.data = horas.data 
        and realizada.pico = horas.pico
order by linha, pico