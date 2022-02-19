from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    @apply_defaults
    def __init__(self,
                 table="",
                 bucket="nanodegreerodcastro",
                 file="",
                 delimiter=",",
                 is_parquet_file= False,
                 *args, **kwargs):
        super(LoadToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.bucket = bucket
        self.file = file
        self.delimiter = delimiter
        self.is_parquet_file = is_parquet_file


    def execute(self, context):
        """
            Copy JSON data from S3 buckets into staging tables.
                - redshift_conn_id: redshift connection ID
                - aws_credentials_id: AWS credentials ID
                - table: Target table on redshift
                - s3_bucket:S3 bucket name where JSON data resides
                - s3_key: S3 key files
                - file_format: source file format
                - region: AWS Region where the source data is located
        """
        
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        if self.is_parquet_file:
            COPY_SQL = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT AS PARQUET;
               """
            
            COPY_STATIONS_SQL = COPY_SQL.format(
                self.table,
                "s3://" + self.bucket + "/" + self.file,
                credentials.access_key,
                credentials.secret_key
            )
            
        else:
            COPY_SQL = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                IGNOREHEADER 1
                DELIMITER '{}'
            """

            COPY_STATIONS_SQL = COPY_SQL.format(
                self.table,
                "s3://" + self.bucket + "/" + self.file,
                credentials.access_key,
                credentials.secret_key,
                self.delimiter    
            )
        
        redshift_hook = PostgresHook("redshift")
        redshift_hook.run(COPY_STATIONS_SQL)
        
        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")

