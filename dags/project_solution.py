from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

# Отримуємо шлях до проєкту з Airflow Variables
# Це робить DAG переносимим між різними середовищами
# Треба встановити цю змінну в UI: Admin -> Variables -> Key: de_project_home, Val: /path/to/your/goit-de-fp
PROJECT_HOME = Variable.get("de_project_home", default_var="/opt/airflow/projects/goit-de-fp")

# Визначаємо шляхи до скриптів
BATCH_SCRIPT_PATH = os.path.join(PROJECT_HOME, "scripts", "part2_batch")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='batch_datalake_pipeline_dag',
    start_date=datetime(2025, 6, 11),
    schedule_interval='@daily', # Запускатиметься щодня
    catchup=False,
    default_args=default_args,
    tags=['goit-de-fp', 'batch', 'final-project'],
    doc_md="""
    ### Batch Datalake ETL Pipeline

    This DAG orchestrates the three steps of building the datalake:
    1.  **landing_to_bronze**: Downloads raw CSV files from an FTP server and saves them as Parquet in the Bronze layer.
    2.  **bronze_to_silver**: Reads data from Bronze, cleans text fields, removes duplicates, and saves to the Silver layer.
    3.  **silver_to_gold**: Reads from Silver, joins the tables, calculates aggregated statistics (avg height and weight), and saves the final analytical dataset to the Gold layer.
    """
) as dag:

    landing_to_bronze = BashOperator(
        task_id='landing_to_bronze',
        bash_command=f"spark-submit {os.path.join(BATCH_SCRIPT_PATH, 'landing_to_bronze.py')}"
    )

    bronze_to_silver = BashOperator(
        task_id='bronze_to_silver',
        bash_command=f"spark-submit {os.path.join(BATCH_SCRIPT_PATH, 'bronze_to_silver.py')}"
    )

    silver_to_gold = BashOperator(
        task_id='silver_to_gold',
        bash_command=f"spark-submit {os.path.join(BATCH_SCRIPT_PATH, 'silver_to_gold.py')}"
    )

    # Визначення послідовності виконання
    landing_to_bronze >> bronze_to_silver >> silver_to_gold