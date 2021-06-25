SELECT 
  data, EXTRACT(HOUR FROM hora_completa) hora, linha, count(distinct ordem) n_veiculos
FROM `rj-smtr.dashboard_monitoramento_sppo.registros_tratada_completa`
where distancia_da_garagem_metros > 600
GROUP BY data, EXTRACT(HOUR FROM hora_completa), linha