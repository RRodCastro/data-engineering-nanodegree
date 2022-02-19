from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 *args, **kwargs):


        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.table} ({self.sql_query})"
        redshift_hook.run(formatted_sql)
