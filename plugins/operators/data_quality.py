from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 self.redshift_conn_id=redshift_conn_id
                 self.aws_credentials_id=aws_credentials_id
                 self.table=table
                 self.s3_bucket=s3_bucket
                 self.s3_key=s3_key
                 self.create_table_sql=create_table_sql
                 self.json_settings=json_settings
                 self.region=region                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.create_table_sql=create_table_sql
        self.json_settings=json_settings
        self.region=region

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')