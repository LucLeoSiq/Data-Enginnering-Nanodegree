from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# With dimension operator, utilize the provided SQL helper class to run data
# transformations. Most of the logic is within the SQL transformations and the
# operator is expected to take as input a SQL statement and target database on
# which to run the query against. You can also define a target table that will
# contain the results of the transformation.

# Dimension loads are often done with the truncate-insert pattern where the
# target table is emptied before the load.

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id=""
                table=""
                sql_stmt=""
                truncate=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id= redshift_conn_id,
        self.table= table,
        self.sql_stmt=sql_stmt
        self.truncate=truncate


    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
