SELECT
  data, EXTRACT(HOUR FROM hora_completa) hora, count(distinct ordem) n_veiculos
FROM `rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_tratada`
where fora_garagem is true AND linha IS NOT NULL
GROUP BY data, EXTRACT(HOUR FROM hora_completa)