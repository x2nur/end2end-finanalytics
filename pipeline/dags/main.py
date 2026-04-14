import logging 
from datetime import datetime, timedelta 
from airflow.sdk import DAG 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator


logger = logging.getLogger(__name__)


with DAG(
    dag_id='finanalytics',
    # schedule='*/10 * * * *', # every 10 minutes for tests 
    schedule=None, # manual run 
    # start_date=datetime.now() - timedelta(minutes=10), 
    default_args={
        'retries': 2,
        'retry_delay': timedelta(seconds=10)
    }
):
    start = EmptyOperator(task_id='start') 

    tx_job_step1 = GlueJobOperator(
        task_id='cleansing_tx',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='Transactions',
        wait_for_completion=True
    )


    start >> tx_job_step1

