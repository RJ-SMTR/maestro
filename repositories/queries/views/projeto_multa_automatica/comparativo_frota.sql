with 
planejada as (
select * 
from rj-smtr.projeto_multa_automatica.frota_planejada 
),

realizada as (
select * 
from rj-smtr.projeto_multa_automatica.frota_realizada
),

merged as (
select  realizada.data as data, 
        realizada.linha as linha,
        realizada.pico as pico,
        frota_operante_media as frota_realizada,
        frota_planejada,
        ROUND(frota_operante_media /frota_planejada,2) as porcentagem_operacao,
from realizada
inner join planejada

on      realizada.data = planejada.data 
        and realizada.linha = planejada.linha 
        and realizada.pico = planejada.pico 
)

SELECT  data, 
        linha,
        pico, 
        frota_realizada, 
        frota_planejada, 
        porcentagem_operacao,
        CASE
                WHEN porcentagem_operacao < 0.8 THEN true
                ELSE false
        END AS multavel
FROM merged