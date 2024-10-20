# BEES Data Engineering – Breweries Case

This project performs the **ETL** (Extraction, Transformation, and Loading) of Ambev brewery data.

## Requirements

- Docker
- Docker Compose

### Steps to Run the Pipeline:

1. Access the case folder:
    ```
    cd case
    ```

2. Start the containers using the command:
    ```
    docker compose up
    ```
   This command should be executed from the `docker-compose.yaml` file located in the `case` folder.

3. Access the Airflow interface:
    - URL: `localhost:8081`
    - Username: `admin`
    - Password: `admin`

4. Turn on the 2 DAGs:
    - `ambev_breweries_pipeline`
    - `ambev_data_quality_pipeline`

5. Trigger the DAG: `ambev_breweries_pipeline`
   Once it finishes, it will automatically trigger the Data Quality pipeline.

![image](https://github.com/user-attachments/assets/9bbfe253-7e49-4221-a242-7036b7173747)

### Monitoring

Two libraries (`libs/logs.py`) were developed for monitoring pipeline failures, which are:
- `def notification_discord(message)` -> This function's purpose is to notify a Discord channel by sending an alert message.

- `def log_callback_fail(context)` -> This function's purpose is to capture the execution status of the DAGs. If the status is 'failed', it will call the Discord notification function.

![image](https://github.com/user-attachments/assets/6183cc13-b189-4c28-841b-22d419a2a764)

### Data Quality

The DAG `ambev_data_quality_pipeline` performs validation on the silver layer of the table. The library used for these validations is ***great_expectations***.

After performing 5 validations, the result is saved in a log file (`/scripts/validation_results.txt`), reporting the ***final validation percentage***.

In the parameter file (`dags/parameters_data_quality.yaml`) of the same DAG, it is possible to set a minimum acceptance value for the table's Data Quality:
`data_quality_percentage: 60.00`

Finally, if the table does not meet the Data Quality standards, it will ***alert/notify*** a Discord channel.

![image](https://github.com/user-attachments/assets/5094cd7e-da3d-48a8-9c89-995c6c76e0b9)



















# PORTUGUÊS PT/BR

# BEES Data Engineering – Breweries Case 

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

5. Triggar a DAG: `ambev_breweries_pipeline`
     Após a finalização ela irá acionar automaticamente a pipeline de DataQuality.

![image](https://github.com/user-attachments/assets/9bbfe253-7e49-4221-a242-7036b7173747)

### Monitoração

Foi desenvolvida 2 libs `(libs/logs.py)` para a monitoração de falhas da pipeline sendo elas:
- `def notification_discord(message)` ->O intuito desta função é notificar um canal do Discord enviando alguma mensagem de alerta.

- `def log_callback_fail(context)` -> O intuito desta função é pegar os status de execuções das DAG's, caso o status seja 'failed' ele irá chamar a função de notificação do discord.

![image](https://github.com/user-attachments/assets/6183cc13-b189-4c28-841b-22d419a2a764)


### Data Quality
 A DAG `ambev_data_quality_pipeline` realiza a validação da camada silver da tabela. A biblioteca utilizada para a realização das validações foi a ***great_expectations***.

Após as 5 validações o resultado é salvo em um arquivo de logs (/scripts/validation_results.txt), informando o ***percentual final das validações***.

Nos arquivo de parametros (dags/parameters_data_quality.yaml) da mesma DAG é possível definir um valor minímo de aceitação do Data Quality da tabela:
`data_quality_percentage: 60.00`

Por fim caso a tabela não esteja nos conformes de Data Quality ela irá ***alertar/notificar*** algum canal no Discord.


![image](https://github.com/user-attachments/assets/5094cd7e-da3d-48a8-9c89-995c6c76e0b9)







