from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
                COPY {}
                FROM '{}'
                credentials 'aws_iam_role={}'
                JSON '{}'
                compupdate off region '{}'
              """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_bucket="",
                aws_iam_role = "",
                json_path="",
                region="",
                *args, 
                **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.aws_iam_role=aws_iam_role
        self.json_path = json_path
        self.region=region
     

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                                                                self.table,
                                                                self.s3_bucket,
                                                                self.aws_iam_role,
                                                                self.json_path,
                                                                self.region
                                                                 )
        redshift.run(formatted_sql)
        




