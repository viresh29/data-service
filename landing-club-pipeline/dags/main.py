from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import importlib
from landing_club_ingest_callble import CsvToDF, SnowflakeCsvUpload

params = importlib.import_module(f'params')


def create_dag(dag_id, config, DEFAULT_ARGS, SCHEDULE_INTERVAL):
    with DAG(dag_id, default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL, description='sample dag') \
            as dag:
        csvtos3_callable = CsvToDF(file_path, file_name, from_date, to_date, bucket_name, s3_connection_id,
                                timeout_counter,
                 timeout_sleep)

        t1 = PythonOperator(task_id='ingest_csv_to_s3',
                            python_callable=csvtos3_callable,
                            provide_context=True)

        ingest_snowflake = SnowflakeCsvUpload(user_name, account_name, warehouse, password, timeout_counter,
                                          timeout_sleep)

        t2 = PythonOperator(task_id='s3_to_snowflake',
                            python_callable=ingest_snowflake,
                            provide_context=True)

        t1 >> t2
    return dag


globals()['landing-club-ingestion'] = create_dag(params.DAG_ID, params.config, params.DEFAULT_ARGS,
                                                 params.SCHEDULE_INTERVAL)
