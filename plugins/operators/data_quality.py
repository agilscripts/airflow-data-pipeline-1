from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 expected_result,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.expected_result = expected_result

    def execute(self, context):
        self.log.info('Starting data quality check')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(self.sql)

        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.sql} returned no results.")

        num_records = records[0][0]
        if num_records != self.expected_result:
            raise ValueError(f"Data quality check failed. {self.sql} returned {num_records} results, "
                             f"but we expected {self.expected_result}.")

        self.log.info('Data quality check passed')
