# Watchdog de pipeline usando Redis

Esse modelo de watchdog assume que as pipelines, ao final de sua execução, escrevam em uma chave arbitrária no Redis, que deve ser a mesma informada no arquivo de configuração do watchdog. Essa chave deve ser definida no YAML do pipeline da seguinte maneira:

```yaml
resources:
  # seus outros resources aqui
  keepalive_key:
    config:
      key: "nome_da_chave"
```

## Configuração

Para configurar os parâmetros no watchdog, altere o arquivo `watchdog_config.yaml` (presente na raiz do repositório). Uma configuração inicial já está inserida. Exemplo de configuração (explicações abaixo):

```yaml
---
config:
  cooldown: 120
  loop_time_delta: 30
services:
  pipelines:
    timeout: 300
    command: docker restart dagster_dagit dagster_daemon dagster_pipelines
    keys:
      - br_rj_riodejaneiro_brt_gps
      - br_rj_riodejaneiro_onibus_gps
```

- `config.cooldown`: tempo de espera antes de iniciar as verificações e após a execução do comando definido para timeout do serviço
- `config.loop_time_delta`: delay entre as verificações
- `config.discord_webhook`: URL do webhook do Discord para notificações do watchdog

Dentro de `services`, você pode definir os serviços que deseja monitorar. No caso desse repositório, o único definido é o `pipelines`, para verificação do devido funcionamento das pipelines do Dagster. Para cada serviço, você pode configurar:

- `timeout`: tempo máximo que um serviço ficará sem escrever na sua respectiva chave no Redis
- `command`: comando a ser executado caso o timeout exceda
- `keys`: uma lista de chaves do Redis (o watchdog irá disparar caso qualquer uma delas exceda o tempo limite)

O watchdog está inserido na stack do docker-compose, com acesso ao daemon do Docker para realizar operações em containers.