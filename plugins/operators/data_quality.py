from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import execute_values
import pandas as pd

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_info_dict=[""],
                 *args, **kwargs):
        """
        :param redshift_conn_id: RedShift Connection ID
        :param aws_credentials_id: AWS Credentials ID
        :param table_info_dict: dict with table name and column that should never be NULL in the table
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_info_dict    = table_info_dict

    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # RedShift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Test each table
        for table_dict in self.table_info_dict:
            table_name = table_dict["table_name"]
            column_that_should_not_be_null = table_dict["not_null"]
            # Check number of records (pass if > 0, else fail)
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table_name} returned no results")
            elif records[0][0] < 1:
                raise ValueError(f"Data quality check failed. {table_name} contained 0 rows")
            else:
                # Now check is NOT NULL columns contain NULL
                null_records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name} WHERE {column_that_should_not_be_null} IS NULL")
                if null_records[0][0] > 0:
                    col = column_that_should_not_be_null
                    raise ValueError(f"Data quality check failed. {table_name} contained {null_records[0][0]} null records for {col}")
                else:
                    self.log.info(f"Data quality on table {table_name} check passed with {records[0][0]} records")
                    