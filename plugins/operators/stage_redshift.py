from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    
    copy_csv_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    copy_json_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_name="",
                 s3_bucket="",
                 s3_key="",
                 s3_format="json",
                 delimiter=",",
                 ignore_headers=1,
                 json_path="auto",
                 *args, **kwargs):
        """
        :param redshift_conn_id: RedShift Connection ID
        :param aws_credentials_id: AWS Credentials ID
        :param table_name: Table Name
        :param s3_bucket: Name of the S3 Bucket
        :param s3_key: Key for partitioning
        :param s3_format: json or csv
        :param delimiter: Delimiter for CSV format
        :param ignore_headers: Flag to ignore headers for CSV files
        :param json_path: auto or you can pass a json path
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id   = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name         = table_name
        self.s3_bucket          = s3_bucket
        self.s3_key             = s3_key
        self.s3_format          = s3_format
        if self.s3_format == "csv":
            self.delimiter = delimiter
            self.ignore_headers = ignore_headers
        if self.s3_format == "json":
            self.json_path = json_path
            
    
    def execute(self, context):
        # AWS Hook
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # RedShift Hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.s3_format == "csv":
            formatted_sql = StageToRedshiftOperator.copy_csv_sql.format(
                self.table_name,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_json_sql.format(
                self.table_name,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
            
        redshift.run(formatted_sql)
