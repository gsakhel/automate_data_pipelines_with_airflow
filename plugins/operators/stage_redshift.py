from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    # Operator for creating tables and load them with data from S3
    
    """
    
    ui_color = '#358140'
    copy_table_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 create_table_sql="",
                 json_settings='',
                 region='us-west-2',
                 #delimiter=",",
                 #ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
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
        #self.delimiter=delimiter
        #self.ignore_headers=ignore_headers

    def execute(self, context):
        self.log.info('StageToRedshiftOperator still being implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"\n s3_key = {self.s3_key}")
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"\n rendered key = {rendered_key}")
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"\n s3_path = {s3_path}")
        
        self.log.info(f"Clearing data from the destination Redshift table: {self.table}")
        redshift.run(f"DROP TABLE IF EXISTS public.{self.table}")
        
        self.log.info(f"Creating table: {self.table} on Redshift")        
        redshift.run(self.create_table_sql)
        
        self.log.info(f"Copy data into table: {self.table}")
        formatted_sql = self.copy_table_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_settings,
            self.region
        )
        redshift.run(formatted_sql)
        
        record_count=redshift.get_first(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f"\nrecord_count: {record_count}")