# Maestro

O dagster pode ser rodado localmente com o make ou utilizando docker.
Ambos os casos utilizam variáveis de ambiente para configuração do BD, webhooks e as pastas dos sensores.

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
```
2. Modifique o arquivo do workspace.yaml para executar com as configurações do docker e inicie seu build
```
cp workspace_docker.yaml workspace.yaml
docker-compose up --build
```

### Utilizando o make
1. Assim como no caso anterior, crie um arquivo para colocar as variáveis de ambiente. Pode chamá-lo de `.env_make`. Além das variáveis anteriores, é preciso configurar mais duas:
```
COMPOSE_PROJECT_NAME=<prefixo das imagens que serão contruídas>
DAGSTER_POSTGRES_USER=<usuario do PSQL>
DAGSTER_POSTGRES_PASSWORD=<senha do PSQL>
DAGSTER_POSTGRES_DB=<BD do PSQL>
DAGSTER_POSTGRES_HOST=<host do PSQL>
BRT_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas ao BRT>
SPPO_DISCORD_WEBHOOK=<webhook do discord para enviar as notificações das pipelines relacionadas aos ônibus>
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