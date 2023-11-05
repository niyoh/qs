from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import ust_scraper

import os

# get dag directory path
dag_path = os.getcwd()

# init default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

scraper_dag = DAG(
    'scraper',
    default_args=default_args,
    description='scraper for UST',
    schedule_interval=timedelta(days=1),
    catchup=False
)

task_scrape = PythonOperator(
    task_id='scrape',
    python_callable=ust_scraper.ust_scraper(),
    dag=scraper_dag
)

end_task = EmptyOperator(
    task_id='end_task',
    dag=scraper_dag
)

task_scrape >> end_task