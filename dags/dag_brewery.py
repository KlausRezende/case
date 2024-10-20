
import yaml
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from libs.log import log_callback_success
from libs.log import log_callback_fail
from libs.log import notification_discord


with open(f'/opt/airflow/dags/parameters_brewery.yaml','r') as f:
   parameters = yaml.safe_load(f)

default_args = {
    'owner': 'Klaus_Rezende'
}

with DAG(
    dag_id=f"{parameters['config']['dag_name']}",
    start_date=datetime(2023, 12, 31), 
    schedule_interval= parameters['config']['schedule_interval'],
    catchup=False,
    default_args=default_args,
    on_success_callback=log_callback_success,
    on_failure_callback=log_callback_fail
) as dag:

    start = EmptyOperator(
        task_id='start_pipeline',
        dag=dag
    )
    for run in parameters['config']['run_scripts']:
        bronze = BashOperator(
            task_id='run_bronze',
            bash_command= run['bronze']
        )

        silver = BashOperator(
            task_id='run_silver',
            bash_command= run['silver']
        )

        gold = BashOperator(
            task_id='run_gold',
            bash_command= run['gold']
        )

        trigger_target = TriggerDagRunOperator(
            task_id = 'trigger_dataquality',
            trigger_dag_id = 'ambev_data_quality_pipeline'
        )

        end = EmptyOperator(
            task_id='finish_pipeline',
            dag=dag
        )

    start >> bronze >> silver >> gold >> trigger_target >> end
    