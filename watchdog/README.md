# Watchdog de pipeline usando Redis

**Disclaimer**: Work in progress. Idealmente, esse readme está atualizado para funcionar com a atual versão encontrada nesse repositório. No entanto, por estar em desenvolvimento, mudanças bruscas poderão ocorrer até o momento em que essa branch se una à master.

Esse modelo de watchdog assume que as pipelines, ao final de sua execução, escrevam em uma chave arbitrária no Redis, que deve ser a mesma informada na configuração do watchdog. Essa chave deve ser definida no YAML do pipeline da seguinte maneira:

```yaml
resources:
  # seus outros resources aqui
  keepalive_key:
    config:
      key: "nome_da_chave"
```

Já para configurar os parâmetros no watchdog, altere o arquivo `watchdog_service.py` de forma a incluir essa(s) chave(s) (cada serviço pode verificar mais de uma chave), seu timeout e o comando shell a ser executado caso esse timeout estoure. Exemplo:

```py
SERVICES = {
    "pipeline_qualquer": {
        "timeout": 5 * MINUTE,
        "keys": [
            "nome_da_chave",
            "nome_da_chave_2",
        ],
        "command": "systemctl restart docker"
    }
}
```

O watchdog deve operar, idealmente, em modo super-usuário e diretamente no OS de origem. Para instalá-lo, altere o arquivo `watchdog_service.service`, corrigindo os paths em `WorkingDirectory` e `ExecStart` e, em seguida, execute o script `install.sh`. Você deve garantir também que todas as dependências descritas em `requirements.txt` estão devidamente instaladas e acessíveis ao usuário que executará o script do watchdog.