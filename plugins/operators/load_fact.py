from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import execute_values
import pandas as pd

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    template_fields = ("filter_key",)

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 sql="",
                 load_mode="append",
                 filter_key=("", ""),
                 *args, **kwargs):
        """
        :param redshift_conn_id: RedShift Connection ID
        :param aws_credentials_id: AWS Credentials ID
        :param table_name: Table Name
        :param sql: SQL Query for loading the fact table
        :param load_mode: Should append to existing data, or on clean table
        :param filter_key: Filter Key for partitioning
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name         = table_name
        self.sql                = sql
        self.load_mode          = load_mode
        self.filter_key         = filter_key
        
    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # RedShift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # Get number of records in the table
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table_name}")
        # Fields and data
        df = redshift.get_pandas_df(self.sql)
        fields = list(df.columns.values)
        data_rows = redshift.get_records(self.sql)
        
        if self.load_mode == "clean":
            # Clear data
            self.log.info(f"Clearing data from {self.table_name} table")
            redshift.run("DELETE FROM {}".format(self.table_name))
            self.log.info(f"Deleted {records[0][0]} records from {self.table_name}")
        else:
            job_execution_ts = self.filter_key[0].format(**context)
            next_job_execution_ts = self.filter_key[1].format(**context)
            filtered_df = df[(df['start_time'] >= job_execution_ts) & (df['start_time'] < next_job_execution_ts)]
            data_rows = [tuple(x) for x in filtered_df.values]
        
        # Populate table
        self.log.info("Populating data to {} table".format(self.table_name))
        redshift.insert_rows(table=self.table_name, 
                                rows=data_rows, 
                                target_fields=fields, 
                                commit_every=1000, 
                                replace=False)
        self.log.info("Inserted {} records to {}".format(len(data_rows), self.table_name))
