SELECT 
    data,
    EXTRACT(HOUR FROM hora_completa) hora,
    consorcio,
    count(distinct ordem) n_veiculos
from `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada` t1
join `rj-smtr.br_rj_riodejaneiro_transporte.linhas_sppo` t2
on t1.linha = t2.linha_completa
where t1.fora_garagem is true
group by 
    t1.data,
    EXTRACT(HOUR FROM hora_completa),
    t2.consorcio