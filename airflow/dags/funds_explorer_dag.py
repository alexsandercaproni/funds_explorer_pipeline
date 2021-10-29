
import sys
sys.path.insert(0, "/Users/alexsandercaproni/Documents/python_projects/scraping_funds_explorer")
from scripts.web_scraping import scraping_funds
from scripts.transform_webpages import extract_funds_information
from scripts.generate_analysis import create_funds_dataframe

import datetime as dt

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
}

with DAG(
    dag_id='funds_explorer_dag',
    default_args=ARGS,
    schedule_interval='0 9 * * *',
    start_date= dt.datetime(2021, 10, 25, 10, 00, 00),
    max_active_runs=1
) as dag:
    extract_funds_operator = PythonOperator(task_id='extract_funds_webpage',
                                  python_callable=scraping_funds)
    extract_funds_information = PythonOperator(task_id='extract_funds_information',
                                        python_callable=extract_funds_information)
    create_funds_analysis_operator = PythonOperator(task_id='create_funds_analysis_operator',
                                    python_callable=create_funds_dataframe)


extract_funds_operator >> extract_funds_information >> create_funds_analysis_operator