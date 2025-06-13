from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Шлях до директорії зі скриптами всередині контейнера Airflow
SCRIPT_PATH = "/opt/airflow/scripts/part2_batch"
# Повний шлях до spark-submit всередині контейнера Airflow
SPARK_SUBMIT_PATH = "/opt/spark/bin/spark-submit"

with DAG(
    dag_id='batch_datalake_pipeline_dag',
    start_date=datetime(2025, 6, 13),
    schedule_interval=None,
    catchup=False,
    tags=['goit-de-fp', 'batch'],
) as dag:

    # ВИПРАВЛЕНО: Використовуємо повний шлях до spark-submit
    landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command=f"{SPARK_SUBMIT_PATH} {os.path.join(SCRIPT_PATH, 'landing_to_bronze.py')}"
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f"{SPARK_SUBMIT_PATH} {os.path.join(SCRIPT_PATH, 'bronze_to_silver.py')}"
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command=f"{SPARK_SUBMIT_PATH} {os.path.join(SCRIPT_PATH, 'silver_to_gold.py')}"
    )

    # Визначення послідовності виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold
