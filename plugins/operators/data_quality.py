from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """ Operator for testing results of SQL queries against a given answer

        args:
            redshift_conn_id  - connection to redshift
            checks            - list of dictionaries with values for 'test_sql' and 'expected_result'. expected_results should be a list of tuples
            comparison_checks - list of dictionaries with values for 'test_sql', 'operator', and 'inequality'. operator is one of '>', '<', or '='

    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 checks="",
                 comparison_checks="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.checks=checks
        self.comparison_checks=comparison_checks

    def execute(self, context):
        self.log.info('DataQualityOperator')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
                
        self.log.info("Beginning checks")
        for i, check in enumerate(self.checks):
            self.log.info(f"Running check #{i}")
            result = redshift.get_records(check['test_sql'])
            self.log.info(f"Result: {result}")
            assert result == check['expected_result'], f"Test Failed. result: {result} != expected_result: {check['expected_result']}"

        self.log.info("Beginning comparison checks")
        for i, check in enumerate(self.comparison_checks):
            self.log.info(f"Running comparison check #{i}")
            result = redshift.get_first(check['test_sql'])
            self.log.info(f"Result: {result}")
            self.log.info(f"Operator: {check['operator']}")
            if check['operator']=='>':
                assert result[0] > check['inequality'], f"Comparison Failed. .result: {result} is not operator: {check['operator']} for inequality: {check['inequality']}"
            elif check['operator']=='<':
                assert result[0] < check['inequality'], f"Comparison Failed. .result: {result} is not operator: {check['operator']} for inequality: {check['inequality']}"
            elif check['operator']=='=':
                assert result[0] == check['inequality'], f"Comparison Failed. .result: {result} is not operator: {check['operator']} for inequality: {check['inequality']}"
            else:
                assert check['operator'] in ['>', '<'], "Use less-than or greater-than for operator"