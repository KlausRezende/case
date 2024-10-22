# BEES Data Engineering – Breweries Case

This project performs the **ETL** (Extraction, Transformation, and Loading) of Ambev brewery data.

![image](https://github.com/user-attachments/assets/67f4b523-6f32-4859-98bc-b8683a91a219)


## Requirements

- Docker
- Docker Compose

### Steps to Run the Pipeline:

1. Clone repo and access the case folder:
    ```
    clone repo && git clone <"url">
    cd case
    ```
2. Clean the folders: (optional)
    ```
    rm -rf bronze_layer/*
    rm -rf silver_layer/*
    rm -rf gold_layer/*
    ```
3. Start the containers using the command:
    ```
    docker compose up
    ```
   This command should be executed from the `docker-compose.yaml` file located in the `case` folder.

4. Access the Airflow interface:
    - URL: `localhost:8081`
    - Username: `admin`
    - Password: `admin`

5. Turn on the 2 DAGs:
    - `ambev_breweries_pipeline`
    - `ambev_data_quality_pipeline`

6. Automatically the DAG: `ambev_breweries_pipeline` will be triggered:
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
![image](https://github.com/user-attachments/assets/b20d9e8c-6567-48cd-91c6-d48dc421d422)


In the parameter file (`dags/parameters_data_quality.yaml`) of the same DAG, it is possible to set a minimum acceptance value for the table's Data Quality:
`data_quality_percentage: 60.00`

Finally, if the table does not meet the Data Quality standards, it will ***alert/notify*** a Discord channel.

![image](https://github.com/user-attachments/assets/5094cd7e-da3d-48a8-9c89-995c6c76e0b9)


### Note
When you activate the two DAGs for the first time, they automatically start running based on the defined start_date. As a result, the data_quality DAG will initially fail due to the absence of files, which is caused by the folder cleaning command in Step 2. However, after the main pipeline finishes executing, the data_quality DAG will run successfully without any issues. So, no need worries about the initial failure, they are expected and the process is functioning as intended.








