from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The stage operator is expected to be able to load any JSON formatted files
# from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement
# based on the parameters provided. The operator's parameters should specify
# where in S3 the file is loaded and what is the target table.

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
