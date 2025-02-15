from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow import AirflowException

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_conn_id='aws_credentials',  # Use the Airflow AWS connection ID
                 redshift_conn_id='redshift_default',
                 s3_bucket='your-bucket-name',
                 s3_key='your-data-file',
                 table_name='your_table_name',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table_name = table_name

    def execute(self, context):
        # Initialize S3 and Redshift hooks
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Log the beginning of the staging process
        self.log.info(f'Staging data from S3 bucket {self.s3_bucket} to Redshift table {self.table_name}')

        # Generate the S3 path and check if the file exists in S3
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
        if not s3_hook.check_for_key(self.s3_key, self.s3_bucket):
            raise AirflowException(f"The file {self.s3_key} does not exist in the S3 bucket {self.s3_bucket}")

        # Create Redshift COPY command to load data from S3
        copy_sql = f"""
        COPY {self.table_name}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{s3_hook.get_credentials().access_key}'
        SECRET_ACCESS_KEY '{s3_hook.get_credentials().secret_key}'
        REGION 'us-west-2' 
        JSON 'auto';
        """

        # Execute the COPY command in Redshift
        redshift_hook.run(copy_sql)

        self.log.info(f"Data from {s3_path} loaded into Redshift table {self.table_name} successfully.")
