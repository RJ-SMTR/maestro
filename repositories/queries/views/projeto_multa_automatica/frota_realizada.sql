select  data,
        linha,
        ROUND(AVG( n_veiculos)) as frota_operante_media,
CASE
    WHEN hora BETWEEN 5 and 8 THEN "manhã" 
    WHEN hora BETWEEN 16 and 19 THEN "noite"
    END AS pico,
FROM rj-smtr.br_rj_riodejaneiro_onibus_gps.registros_agg_data_hora_linha as t
WHERE (t.hora between 5 and 8 OR t.hora between 16 and 19)
GROUP BY pico,data,linha
ORDER BY linha