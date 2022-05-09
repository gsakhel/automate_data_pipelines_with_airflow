from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):
    """This operator will create our dimension tables on Redshift using our staging tables.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 create_table_sql="",
                 insert_table_sql="",
                 json_settings='auto',
                 region='us-west-2',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        
        # Do we need both?
        self.create_table_sql=create_table_sql
        self.insert_table_sql=insert_table_sql
        self.json_settings=json_settings
        self.region=region

    def execute(self, context):
        self.log.info('LoadDimensionOperator implemented soon')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift = PostgresHook(posgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Drop then create table: {self.table}")
        redshift.run(f"DROP TABLE IF EXISTS {self.table}")
        redshift.run(self.create_table_sql)

        self.log.info(f"Instert data into table: {self.table}")
        redshift.run(self.insert_table_sql)