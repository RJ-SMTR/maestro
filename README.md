# Maestro

O dagster pode ser rodado localmente com o make ou utilizando docker.
Ambos os casos utilizam variáveis de ambiente para configuração do BD, webhooks e as pastas dos sensores.
Caso seja de interesse, é possível configurar o watchdog das pipelines. Instruções no diretório `watchdog/`.

### Utilizando docker
1. Crie um arquivo chamado `.env` na raiz do projeto com as seguintes variáveis de ambiente:
```
COMPOSE_PROJECT_NAME=<prefixo das imagens que serão contruídas>
DAGSTER_POSTGRES_USER=<usuario do PSQL>
DAGSTER_POSTGRES_PASSWORD=<senha do PSQL>
DAGSTER_POSTGRES_DB=<BD do PSQL>
DAGSTER_POSTGRES_HOST=<host do PSQL>
BRT_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas ao BRT>
SPPO_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas aos ônibus>
WDT_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações do watchdog>
QUERIES_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas às queries>
BQ_PROJECT_NAME=<nome do projeto do BigQuery (default é rj-smtr-dev)>
```
2. O build assume que o repositório 'maestro' foi clonado em /home, caso tenha clonado em outro local, mude as
seguintes linhas no arquivo docker-compose.yaml:
```
linha 32 - 34 /home/${USER}/maestro/repositories:/opt/dagster/app/repositories ->> /home/${USER}/<clone_path>/repositories:/opt/dagster/app/repositories

linha 129 /home/${USER}/maestro/watchdog_config.yaml:/app/watchdog_config.yaml:ro ->> /home/${USER}/projects/smtr-maestro/watchdog_config.yaml:/app/watchdog_config.yaml:ro
```
3. É preciso subir uma instância local do postgresql para que o build funcione. Use o comando:
```
docker run -itd --name postgres -e POSTGRES_PASSWORD=123456 --restart unless-stopped -p 5432:5432 postgres:13
```

4. Altere as variáveis de ambiente de acordo no arquivo .env:
```
DAGSTER_POSTGRES_USER=postgres
DAGSTER_POSTGRES_PASSWORD=123456
DAGSTER_POSTGRES_DB=postgres
```
Altere também a variável DAGSTER_POSTGRES_HOST com o seu endereço de IP

5. Modifique o arquivo do workspace.yaml para executar com as configurações do docker e inicie seu build
```
cp workspace_docker.yaml workspace.yaml
docker-compose up --build
```

6. Reestarte o sistema com
```
docker stop $(docker ps -a -q)
docker-compose up --build -d
```

7. Cheque se o build funcionou usando o comando `docker ps`, a saída deverá ser similar à:
```
CONTAINER ID   IMAGE                    COMMAND                  CREATED         STATUS         PORTS                    NAMES
b59080461555   test_dagster_daemon      "dagster-daemon run"     8 seconds ago   Up 6 seconds                            dagster_daemon
4f47bc455f72   test_dagster_dagit       "dagit -h 0.0.0.0 -p…"   8 seconds ago   Up 6 seconds   0.0.0.0:3000->3000/tcp   dagster_dagit
6fcea03043a2   test_watchdog            "python3 watchdog.py"    8 seconds ago   Up 6 seconds                            watchdog
85fae7f16cc6   test_dagster_pipelines   "dagster api grpc -h…"   9 seconds ago   Up 7 seconds   0.0.0.0:4000->4000/tcp   dagster_pipelines
652e9138ede5   redis:6                  "docker-entrypoint.s…"   9 seconds ago   Up 7 seconds   0.0.0.0:6379->6379/tcp   redis
c4bafe3fbb0a   postgres:13              "docker-entrypoint.s…"   6 minutes ago   Up 6 minutes   0.0.0.0:5432->5432/tcp   postgres
```

### Utilizando o make

**Disclaimer**: Ainda não há compatibilidade do watchdog para o deployment usando `make`. Se isso é uma demanda, favor submeter uma issue.

1. Assim como no caso anterior, crie um arquivo para colocar as variáveis de ambiente. Pode chamá-lo de `.env_make`. Além das variáveis anteriores, é preciso configurar mais duas:
```
COMPOSE_PROJECT_NAME=<prefixo das imagens que serão contruídas>
DAGSTER_POSTGRES_USER=<usuario do PSQL>
DAGSTER_POSTGRES_PASSWORD=<senha do PSQL>
DAGSTER_POSTGRES_DB=<BD do PSQL>
DAGSTER_POSTGRES_HOST=<host do PSQL>
BRT_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas ao BRT>
SPPO_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas aos ônibus>
WDT_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações do watchdog>
QUERIES_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas às queries>
BQ_PROJECT_NAME=<nome do projeto do BigQuery (default é rj-smtr-dev)>
RDO_DATA=<pasta que o sensor dos dados de RDO deve monitorar>
GTFS_DATA=<pasta que o sensor dos dados de GTFS deve monitorar>
```

2. Modifique o arquivo do `workspace.yaml` para executar com as configurações do make
```
cp workspace_make.yaml workspace.yaml
```

3. Rode com
```
make run-daemon
make run-dagit
```

### Algumas premissas do projeto
1. O projeto usa o PostgreSQL como banco de dados. É possível trocar essa configuração, alterando o arquivo `.dagster_workspace/dagster.yaml`. Se não for utilizar o PSQL, pode remover essas variáveis de ambiente do seu `.env_make`. É altamente recomendável a sua utilização com docker.

2. Na configuração do docker, as pastas `/home/$(USER)/RDO_DATA` e `/home/$(USER)/GTFS_DATA` são utilizadas para monitoramento dos respectivos sensores. Essas pastas podem ser alteradas na construção dos volumes dos dockers no arquivo docker-compose.yaml.

3. Para subir arquivos no Google Storage e no Big Query, crie a pasta `.basedosdados` (versão com Make) ou `.basedosdados_dagster` (versão com docker) com as respectivas credenciais.

4. Existe uma variável de ambiente chamada DATA_FOLDER que pode ser utilizada para trocar o local onde os dados são salvos. Por padrão, eles são salvos na pasta `data/`.
