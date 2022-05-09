from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_table_sql = """
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON {}
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
                 json_settings='auto',
                 region='us-west-2',
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

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        redshift.run(f"DROP TABLE IF EXISTS {self.table}")

        # Might not be needed if table gets created from COPY
        redshift.run(self.create_table_sql)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{self.s3_bucket}/{rendered_key}"
        
        # Or is it formatted_sql= StageToRedshiftOperator.copy_table_sql.......
        formatted_sql = self.copy_table_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_settings,
            self.region
        )
        redshift.run(formatted_sql)


    # # STAGING TABLES
    # staging_events_copy = ("""
    #     COPY staging_events
    #     FROM {}
    #     CREDENTIALS 'aws_iam_role={}'
    #     JSON {}
    #     region '{}'
    # """).format(config.get('S3','LOG_DATA'), 
    #             config.get('IAM_ROLE', 'ARN'), 
    #             config.get('S3','LOG_JSONPATH'),
    #             config.get('AWS','REGION')
    #             )

    # staging_songs_copy = ("""
    #     COPY staging_songs
    #     FROM {}
    #     CREDENTIALS 'aws_iam_role={}'
    #     JSON 'auto'
    #     region '{}'
    # """).format(config.get('S3', 'SONG_DATA'),
    #             config.get('IAM_ROLE', 'ARN'),
    #             config.get('AWS', 'REGION')
    #             )