from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ Operator for creating and loading dimension tables
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 create_table_sql="",
                 table_insert_sql="",
                 append_only=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.create_table_sql=create_table_sql
        self.table_insert_sql=table_insert_sql
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.append_only=append_only

    def execute(self, context):
        self.log.info('LoadDimensionOperator implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_only:
            self.log.info(f"Drop then create table: {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS public.{self.table}")
            redshift.run(self.create_table_sql)

        self.log.info(f"Instert data into table: {self.table}")
        redshift.run(self.table_insert_sql)
