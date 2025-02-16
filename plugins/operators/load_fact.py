from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..udacity_helpers import sql_queries
from ..udacity_helpers.sql_queries import SqlQueries


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_conn_id, *args, **kwargs):
        """
        Initializes the operator with the required parameters for Redshift connection
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Executes the task of loading the fact table in Redshift using a SQL query
        """
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Loading songplays fact table")
        
        sql_query = sql_queries.songplay_table_insert

        redshift_hook.run(sql_query)

        self.log.info("Songplays fact table loaded successfully.")
