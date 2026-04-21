from io import TextIOWrapper, BytesIO
import logging 
import json
import csv
from datetime import datetime, timedelta 
from airflow.sdk import DAG 
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Connection 
import boto3 


logger = logging.getLogger(__name__)


raw_data_s3 = 's3://end2end-finanalytics-811710375817-eu-north-1-an/data/raw'
dest_data_s3 = 's3://end2end-finanalytics-811710375817-eu-north-1-an/data/intermediate'


with DAG(
    dag_id='finanalytics',
    # schedule='*/10 * * * *', # every 10 minutes for tests 
    schedule='@daily', # manual run 
    start_date=datetime.strptime('2026-04-13', '%Y-%m-%d'),
    default_args={
        'retries': 0,
        'retry_delay': timedelta(seconds=10)
    }
):
    start = EmptyOperator(task_id='start') 

    # todo: extract from RDS to a specific s3 path with data_interval_start name

    tx_job_step1 = GlueJobOperator(
        task_id='cleansing_tx_step1',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='Transactions-script',
        wait_for_completion=True,
        script_args={
            '--src_data_s3': raw_data_s3 + '/transactions/{{ data_interval_start | ds }}',
            '--dest_data_s3': dest_data_s3 + '/transactions-step1/{{ data_interval_start | ds }}',
            '--missing_zipcodes_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes/{{ data_interval_start | ds }}',
        }
    )

    resolve_missing_zipcodes_lambda = LambdaInvokeFunctionOperator(
        task_id='resolve_missing_zipcodes_lambda',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        function_name='resolve_missing_zipcodes',
        payload=json.dumps({ 
            'missing_zipcodes_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes/{{ data_interval_start | ds }}',
            'output_folder_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes-result/{{ data_interval_start | ds }}'
        })
    )


    tx_job_step2 = GlueJobOperator(
        task_id='cleansing_tx_step2',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='Transactions-step2-script',
        wait_for_completion=True,
        script_args={
            '--src_tx_data_s3': dest_data_s3 + '/transactions-step1/{{ data_interval_start | ds }}',
            '--dest_tx_data_s3': dest_data_s3 + '/transactions-step2/{{ data_interval_start | ds }}',
            '--src_zipcodes_data_s3': dest_data_s3 + '/transactions-step1-missing-zipcodes-result/{{ data_interval_start | ds }}',
        }
    )


    # todo: overwrite current tx data in staging instead append

    copy_tx_to_redshift = RedshiftDataOperator(
        task_id='copy_tx_to_redshift_staging',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        workgroup_name='default-workgroup',
        database='dev',
        sql=f"""
            COPY "dev"."stage"."transactions"
            FROM '{dest_data_s3}/transactions-step2/{{{{ data_interval_start | ds }}}}'
            iam_role default 
            region 'eu-north-1'
            parquet
        """,
        wait_for_completion=True
    )

    
    user_job = GlueJobOperator(
        task_id='cleansing_user',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='User-script',
        wait_for_completion=True,
        script_args={
            '--src_users_s3': raw_data_s3 + '/users/{{ data_interval_start | ds }}',
            '--dest_users_s3': dest_data_s3 + '/users/{{ data_interval_start | ds }}',
        }
    )


    # todo: delete + insert for users table

    copy_users_to_redshift = RedshiftDataOperator(
        task_id='copy_users_to_redshift_staging',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        workgroup_name='default-workgroup',
        database='dev',
        sql=f"""
            COPY "dev"."stage"."users"
            FROM '{dest_data_s3}/users/{{{{ data_interval_start | ds }}}}'
            iam_role default 
            region 'eu-north-1'
            parquet
        """,
        wait_for_completion=True
    )

    

    cards_job = GlueJobOperator(
        task_id='cleansing_cards',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='Cards-script',
        wait_for_completion=True,
        script_args={
            '--src_cards_s3': raw_data_s3 + '/cards/{{ data_interval_start | ds }}',
            '--dest_cards_s3': dest_data_s3 + '/cards/{{ data_interval_start | ds }}',
        }
    )


    copy_cards_to_redshift = RedshiftDataOperator(
        task_id='copy_cards_to_redshift_staging',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        workgroup_name='default-workgroup',
        database='dev',
        sql=f"""
            COPY "dev"."stage"."cards"
            FROM '{dest_data_s3}/cards/{{{{ data_interval_start | ds }}}}'
            iam_role default 
            region 'eu-north-1'
            parquet
        """,
        wait_for_completion=True
    )



    def mcc_codes_json_to_csv(**context):
        src = context['templates_dict']['src_mcc_codes']
        dest = context['templates_dict']['dest_mcc_codes']

        s3h = S3Hook(aws_conn_id='finanalytics_aws')
        buck, key = s3h.parse_s3_url(src)
        content = s3h.read_key(key=key, bucket_name=buck)

        data: dict[str, str] = json.loads(content)
        rows = []
        for k,v in data.items():
            rows.append((k,v))

        with (BytesIO() as buf, TextIOWrapper(buf) as txtf):
            wr = csv.writer(txtf)
            wr.writerow(['mcc','desc'])
            wr.writerows(rows)
            txtf.flush()

            buck, key = s3h.parse_s3_url(dest)
            s3h.load_bytes(buf.getvalue(), key, bucket_name=buck)


    prepare_mcc_codes = PythonOperator(
        task_id='mcc_codes_json_to_csv',
        python_callable=mcc_codes_json_to_csv,
        templates_dict={
            'src_mcc_codes': f'{raw_data_s3}/mcc-codes/{{{{ data_interval_start | ds }}}}/mcc_codes.json',
            'dest_mcc_codes': f'{dest_data_s3}/mcc-codes/{{{{ data_interval_start | ds }}}}/mcc_codes.csv'
        }
    )


    clean_mcc_codes_glue_job = GlueJobOperator(
        task_id='cleansing_mcc_codes',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        job_name='MccCodes-script',
        wait_for_completion=True,
        script_args={
            '--src_mcc_codes_s3': dest_data_s3 + '/mcc-codes/{{ data_interval_start | ds }}',
            '--dest_mcc_codes_s3': dest_data_s3 + '/mcc-codes-step2/{{ data_interval_start | ds }}',
        }
    )


    copy_mcc_codes_to_redshift = RedshiftDataOperator(
        task_id='copy_mcc_codes_to_redshift_staging',
        aws_conn_id='finanalytics_aws',
        region_name='eu-north-1',
        workgroup_name='default-workgroup',
        database='dev',
        sql=f"""
            COPY "dev"."stage"."mcc_codes"
            FROM '{dest_data_s3}/mcc-codes-step2/{{{{ data_interval_start | ds }}}}'
            iam_role default 
            region 'eu-north-1'
            parquet
        """,
        wait_for_completion=True
    )

    con: Connection = Connection.get('dwh_redshift')

    dbt_run = BashOperator(
        task_id='dbt_run_fill_dwh',
        env={ 
            'DBT_PROJECT_DIR': '{{ task.dag.folder }}/finanalytics',
            'DBT_PROFILES_DIR': '{{ task.dag.folder }}/finanalytics',
            'DBT_HOST': con.host,
            'DBT_USER': con.login,
            'DBT_PASSWORD': con.password
        }, # pyright: ignore[reportArgumentType]
        bash_command="""
            source {{ task.dag.folder }}/finanalytics/.venv/bin/activate
            dbt run -s tag:dwh --vars '{ interval_start: "{{ data_interval_start | ds }}" }' 
        """
    )


    dbt_retry = BashOperator(
        task_id='dbt_retry_fill_dwh',
        trigger_rule='all_failed',
        env={ 
            'DBT_PROJECT_DIR': '{{ task.dag.folder }}/finanalytics',
            'DBT_PROFILES_DIR': '{{ task.dag.folder }}/finanalytics',
            'DBT_HOST': con.host,
            'DBT_USER': con.login,
            'DBT_PASSWORD': con.password
        }, # pyright: ignore[reportArgumentType]
        bash_command="""
            source {{ task.dag.folder }}/finanalytics/.venv/bin/activate
            dbt retry -s tag:dwh --vars '{ interval_start: "{{ data_interval_start | ds }}" }' 
        """
    )


    dbt_done = EmptyOperator(task_id='dbt_done', trigger_rule='one_success')


    end = EmptyOperator(task_id='end')


    start >> tx_job_step1 >> resolve_missing_zipcodes_lambda >> tx_job_step2 >> copy_tx_to_redshift

    copy_tx_to_redshift >> user_job >> copy_users_to_redshift

    copy_users_to_redshift >> cards_job >> copy_cards_to_redshift

    copy_cards_to_redshift >> prepare_mcc_codes >> clean_mcc_codes_glue_job >> copy_mcc_codes_to_redshift

    copy_mcc_codes_to_redshift >> dbt_run >> dbt_retry

    [dbt_run, dbt_retry] >> dbt_done >> end

