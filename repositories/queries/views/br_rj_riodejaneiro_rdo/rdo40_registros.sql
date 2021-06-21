SELECT 'brt' as origem, * from `rj-smtr.br_rj_riodejaneiro_rdo.brt_rdo40_registros` 
UNION ALL 
select 'sppo' as origem, * from `rj-smtr.br_rj_riodejaneiro_rdo.onibus_rdo40_registros`;