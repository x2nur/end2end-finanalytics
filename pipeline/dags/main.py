import logging 
import json
from datetime import datetime, timedelta 
from airflow.sdk import DAG 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from botocore import regions


logger = logging.getLogger(__name__)


raw_data_s3 = 's3://end2end-finanalytics-811710375817-eu-north-1-an/data/raw'
dest_data_s3 = 's3://end2end-finanalytics-811710375817-eu-north-1-an/data/intermediate'


with DAG(
    dag_id='finanalytics',
    # schedule='*/10 * * * *', # every 10 minutes for tests 
    schedule='@daily', # manual run 
    start_date=datetime.strptime('2026-04-13', '%Y-%m-%d'),
    default_args={
        'retries': 2,
        'retry_delay': timedelta(seconds=10)
    }
):
    start = EmptyOperator(task_id='start') 

    # todo: extract from RDS to a specific s3 path with data_interval_start name

    tx_job_step1 = GlueJobOperator(
        task_id='cleansing_tx',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='Transactions-script',
        wait_for_completion=True,
        script_args={
            '--src_data_s3': raw_data_s3 + '/transactions/{{ data_interval_start | ds }}',
            '--dest_data_s3': dest_data_s3 + '/transactions-step1/{{ data_interval_start | ds }}',
            '--missing_zipcodes_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes/{{ data_interval_start | ds }}',
            'test': 'test:{{logical_date}}'
        }
    )

    resolve_missing_zipcodes_lambda = LambdaInvokeFunctionOperator(
        task_id='resolve_missing_zipcodes',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        function_name='resolve_missing_zipcodes',
        payload=json.dumps({ 
            'missing_zipcodes_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes/{{ data_interval_start | ds }}',
            'output_folder_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes-result/{{ data_interval_start | ds }}'
        })
    )


    start >> tx_job_step1 >> resolve_missing_zipcodes_lambda

