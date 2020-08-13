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
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.table=table

    def execute(self, context):
        redshift= PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            records = redshift.getrecords(f"SELECT COUNT(*) FROM {table}")
            # Checks if table has any data
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")

            n_records = records[0][0]
            if n_records == 0:
                self.log.error(f"No records present in destination table {table}")
                raise ValueError(f"No records present in destination {table}")
            self.log.info(f"Data quality on table {table} check passed with {n_records} records")
