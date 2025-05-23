import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import  BashOperator

path = os.path.expanduser('~/airflow_hw')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        default_args=args,
) as dag:

    first = BashOperator(
        task_id='welcome',
        bash_command='echo "welcome!"',
        dag=dag
    )

    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag
    )

    predict = PythonOperator(
        task_id='predict',
        python_callable=predict,
    )

    first>>pipeline>>predict






