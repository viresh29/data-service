from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
import importlib
from landing_club_ingest_callble import CsvToDF, SnowflakeCsvUpload
import os

params = importlib.import_module(f'params')


def create_dag(dag_id, config, DEFAULT_ARGS, SCHEDULE_INTERVAL):
    with DAG(dag_id, default_args=DEFAULT_ARGS, schedule_interval=SCHEDULE_INTERVAL, description='sample dag') \
            as dag:
        csvtos3_callable = CsvToDF(config['file_path'],
                                   config['file_name'],
                                   config['AWS']['BUCKET'],
                                   config['AWS']['s3_connection_id'],
                                   config['AWS']['TIMEOUT_COUNTER'],
                                   config['AWS']['TIMEOUT_SLEEP'])

        t1 = PythonOperator(task_id='ingest_csv_to_s3',
                            python_callable=csvtos3_callable,
                            provide_context=True)

        ingest_snowflake = SnowflakeCsvUpload(config['SNOWFLAKE']['USER'],
                                              config['SNOWFLAKE']['ACCOUNT'],
                                              config['SNOWFLAKE']['WAREHOUSE'],
                                              os.getenv('SNOWFLAKE_PASSWORD'),
                                              config['SNOWFLAKE']['TIMEOUT_COUNTER'],
                                              config['SNOWFLAKE']['TIMEOUT_SLEEP'],
                                              **config['SNOWFLAKE'])

        t2 = PythonOperator(task_id='s3_to_snowflake',
                            python_callable=ingest_snowflake,
                            provide_context=True)

        t1 >> t2
    return dag


globals()['landing-club-ingestion'] = create_dag(params.DAG_ID, params.config, params.DEFAULT_ARGS,
                                                 params.SCHEDULE_INTERVAL)
