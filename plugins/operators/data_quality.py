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
                 redshift_conn_id="",
                 test_sql="",
                 test_answer="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.test_sql=test_sql
        self.test_answer=test_answer

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"\nGiven Results: {self.test_answer}")
        query_results = redshift.get_records(self.test_sql)
        self.log.info(f"\nResults: {query_results}")
        assert query_results == self.test_answer, f"Test Failed. querry_results: {query_results} != test_answer: {self.test_answer}"
        