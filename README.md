# ETL BEES 

Este projeto realiza o **ETL** (Extração, Transformação e Carga) de dados de cervejarias da ambev.

## Requisitos

- Docker
- Docker Compose


### Passos para execução da Pipeline:

1. Acesse a pasta case
    ```
    cd case
    ```

2. Suba os containers usando o comando:
    ```
    docker compose up
    ```
   Esse comando deve ser executado a partir do arquivo `docker-compose.yaml` contido na pasta `case`.


3. Acesse a interface do Airflow:
    - URL: `localhost:8081`
    - Usuário: `admin`
    - Senha: `admin`

4. Ligue as 2 DAG's:
    - `ambev_breweries_pipeline`
    - `ambev_data_quality_pipeline`

### Monitoração

Foi desenvolvida 2 libs `(libs/logs.py)` para a monitoração de falhas da pipeline sendo elas:
- `def notification_discord(message)` ->O intuito desta função é notificar um canal do Discord enviando alguma mensagem de alerta.

- `def log_callback_fail(context)` -> O intuito desta função é pegar os status de execuções das DAG's, caso o status seja 'failed' ele irá chamar a função de notificação do discord.


### Data Quality
 A DAG `ambev_data_quality_pipeline` realiza a validação da camada silver da tabela. A biblioteca utilizada para a realização das validações foi a ***great_expectations***.

Após as 5 validações o resultado é salvo em um arquivo de logs (/notebooks/validation_results.txt), informando o ***percentual final das validações***.

Nos arquivo de parametros (dags/parameters_data_quality.yaml) da mesma DAG é possível definir um valor minímo de aceitação do Data Quality da tabela:
`data_quality_percentage: 60.00`

Por fim caso a tabela não esteja nos conformes de Data Quality ela irá ***alertar/notificar*** algum canal no Discord.







