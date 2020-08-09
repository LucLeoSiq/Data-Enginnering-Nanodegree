from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The data quality operator, which is used to run checks on the data itself.
# The operator's main functionality is to receive one or more SQL based test
# cases along with the expected results and execute the tests.

# For each the test, the test result and expected result needs to be checked
# and if there is no match, the operator should raise an exception and the
# task should retry and fail eventually.

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
