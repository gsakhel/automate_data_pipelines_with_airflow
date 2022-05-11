from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):
    """Operator for building our fact table
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 #aws_credentials_id="",
                 table="",
                 create_table_sql="",
                 table_insert_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        #self.aws_credentials_id=aws_credentials_id
        self.table=table
        
        # Do we need both?
        self.create_table_sql=create_table_sql
        self.table_insert_sql=table_insert_sql

    def execute(self, context):
        self.log.info('LoadFactOperator implemented')
        
        # Are these even necessary?
        #aws_hook = AwsHook(self.aws_credentials_id)
        #credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Drop then create table: {self.table}")
        redshift.run(f"DROP TABLE IF EXISTS public.{self.table}")
        redshift.run(self.create_table_sql)

        self.log.info(f"Insert data into table: {self.table}")
        redshift.run(self.table_insert_sql)
        self.log.info(f"Finished Inserting to {self.table}")