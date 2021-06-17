SELECT 
    data,
    EXTRACT(HOUR FROM hora_completa) hora,
    consorcio,
    count(distinct ordem) n_veiculos
from `rj-smtr.dashboard_monitoramento_sppo.registros_tratada_completa` t1
join `rj-smtr.br_rj_riodejaneiro_transporte.linhas_sppo` t2
on t1.linha = t2.linha_completa
where t1.distancia_da_garagem_metros > 600
group by 
    t1.data,
    EXTRACT(HOUR FROM hora_completa),
    t2.consorcio