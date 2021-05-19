# Pipelines

Cada pasta representa uma pipeline. Para modificar as configurações padrão daquela pipeline, altere o arquivo `.yaml` dentro de cada pasta. Segue uma descrição de cada pipeline disponível:

1. `br_rj_riodejaneiro_brt_gps`: recupera as informações de GPS dos BRTs da cidade do Rio de Janeiro. Os dados são recuperados a cada minuto, processados e salvos no Google Cloud Storage.

2. `br_rj_riodejaneiro_gtfs_brt`: calcula as viagens realizadas pelo BRT por dia.

3. `br_rj_riodejaneiro_gtfs_planned`: processa e salva no Google Cloud Storage o GTFS planejado para as viagens de BRT e ônibus da cidade do Rio de Janeiro. Calcula também as viagens planejadas a partir do GTFS.

4. `br_rj_riodejaneiro_onibus_gps`: recupera as informações de GPS dos ônibus da cidade do Rio de Janeiro. Os dados são recuperados a cada minuto, processados e salvos no Google Cloud Storage.

5. `br_rj_riodejaneiro_rdo`: processa e salva no Google Cloud Storage o Relatório Diário de Operação (RDO) dos diversos transportes da cidade do Rio de Janeiro.