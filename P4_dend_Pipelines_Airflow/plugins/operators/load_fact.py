from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# With fact operator, you can utilize the provided SQL helper class to run data
# transformations. Most of the logic is within the SQL transformations and the
# operator is expected to take as input a SQL statement and target database on
# which to run the query against. You can also define a target table that will
# contain the results of the transformation.


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id=""
                table=""
                slq_stmt=""
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
