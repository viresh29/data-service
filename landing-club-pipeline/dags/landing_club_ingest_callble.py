import logging
import snowflake.connector
from airflow.hooks.S3_hook import S3Hook
from decorators import (
    s3_retry_decorator, snowflake_retry_decorator, logging_decorator)
import pandas as pd


class CsvToS3:

    def __init__(self, bucket_name, s3_connection_id='aws_default', timeout_counter=None, timeout_sleep=None):
        logging.info('s3_connection_id  ' + str(s3_connection_id))
        self.hook = S3Hook(aws_conn_id=s3_connection_id)
        self.bucket_name = bucket_name
        self.timeout_counter = timeout_counter
        self.timeout_sleep = timeout_sleep

    @s3_retry_decorator
    def upload_file_to_s3_with_hook(self, filename):
        logging.info('Uploading file to s3 bucket from: ' + filename + ' to '
                     + self.bucket_name + ' and be named there ')
        self.hook.load_file(filename=filename,
                            bucket_name=self.bucket_name, replace=True)

        logging.info('Csv {} uploaded to s3 bucket {}'.format(
            filename, self.bucket_name))
        return filename


class CsvToDF:

    def __init__(self, file_path, file_name, from_date, to_date, bucket_name, s3_connection_id, timeout_counter,
                 timeout_sleep):
        self.file_path = file_path
        self.from_date = from_date
        self.to_date = to_date
        self.file_name = file_name
        self.s3 = CsvToS3(bucket_name=bucket_name,
                          timeout_counter=timeout_counter, timeout_sleep=timeout_sleep)

    # @TODO use from and to dates to filter the data where ever we get from

    # def get_current_date(self, execution_date: datetime) -> datetime:
    #     from_date = execution_date.replace(
    #         hour=0, minute=0, second=0, microsecond=0)
    #     to_date = from_date + timedelta(days=1)
    #     return from_date, to_date
    # @TODO add the logic here to get the data from file or api using from and to dates to get periodically updates
    def replace_df_to_csv(self, dataframe):
        return dataframe.to_csv(index=False)

    def file_format(self, from_date, to_date):
        return self.file_name.replace(from_date=from_date, to_date=to_date)

    def execute(self, execution_date):
        from_date, to_date = self.get_current_date(execution_date)
        df = pd.read_csv(self.file_path)
        csv = self.replace_df_to_csv(df)
        file = self.file_format(csv, from_date, to_date)

        self.s3.upload_file_to_s3_with_hook(filename=file)
        # return file name for next task to pick up.
        return file


class SnowflakeCsvUpload:

    def __init__(self, user_name, account_name, warehouse, password, timeout_counter, timeout_sleep, **kwargs):

        self.user_name = user_name
        self.account_name = account_name
        self.warehouse = warehouse
        self.password = password
        self.timeout_counter = timeout_counter
        self.timeout_sleep = timeout_sleep
        self.connection_status = False
        self.cs = None
        self.db_structure = dict(**kwargs)

    def _establish_conn_with_db(self):

        logging.info('Connect to Snowflake')

        connection = snowflake.connector.connect(
            user=self.user_name,
            account=self.account_name,
            warehouse=self.warehouse,
            password=self.password,
            autocommit=True)

        self.cs = connection.cursor()
        self.connection_status = True

        logging.info('Snowflake connection has been established')

    @snowflake_retry_decorator
    def perform_dml_statement(self,
                              query=None,
                              close_connection_after: bool = True):

        if not self.connection_status:
            logging.info('Connection is not established.')
            self._establish_conn_with_db()
            logging.info('Connection is established')

        logging.info('Performing new query to snowflake')

        format_of_query = isinstance(query, list)
        if format_of_query:
            for que in query:
                self._executing_statement(que)
        elif query:
            self._executing_statement(query)

        logging.info('Snowflake has been already queried')

        data = self.cs.fetchall()

        if close_connection_after:
            logging.info(
                'Function was triggered with parameter for closing connection')
            self.close_connection()

    @snowflake_retry_decorator
    def close_connection(self):
        if self.connection_status:
            self.cs.close()
            self.connection_status = False

        logging.info('Connection to snowflake is closed')

    def _executing_statement(self, statement):
        try:
            self.cs.execute(statement)
        except Exception:
            raise ValueError('SQL statement include syntax errors.')

    @logging_decorator
    def execute(self, table_name, task_id, file_format='STANDARDCSVFORMAT', **kwargs):
        ti = kwargs['ti']
        file_name = ti.xcom_pull(
            task_ids=task_id)

        logging.info("FileName - {}".format(file_name))

        dml_instruction = ['USE DATABASE "{}"'.format(
            self.db_structure.get('DATABASE'))]

        for file in file_name:
            # SQL statement to insert data from file to landing tables
            dml_instruction.append('''COPY INTO "{database_name}"."{schema_name}"."{table_name}"
                                                        FROM @"{schema_name}"."{stage_name}" FILES = ('{file_name}')
                                                        FILE_FORMAT = "{database_name}"."{schema_name}"."{file_format}"
                                                        ON_ERROR = "ABORT_STATEMENT" '''.format(
                database_name=self.db_structure.get('DATABASE'),
                schema_name=self.db_structure.get('SCHEMA'),
                table_name=table_name,
                stage_name=self.db_structure.get('STAGE'),
                file_name=file,
                file_format=file_format))

            logging.info("dml statement - {}".format(dml_instruction))

        try:
            # executing above sql_statement and committing changes
            self.perform_dml_statement(query=dml_instruction, commit=True)
        except ValueError:
            logging.error('Error during loading files: {files}'.format(
                files=','.join(file_name)))
        logging.info('{} - {} loaded csv into {}, files_number'.format(file_name[0], file_name[-1],
                                                                       str(table_name) + 'Staging', len(file_name)))
