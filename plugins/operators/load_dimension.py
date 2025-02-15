from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 table_name,
                 sql_query,
                 append_mode=False,
                 *args, **kwargs):
        """
        :param redshift_conn_id: Redshift connection id
        :param table_name: Target table name
        :param sql_query: SQL query to execute
        :param append_mode: Boolean indicating if data should be appended or truncated
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query
        self.append_mode = append_mode

    def execute(self, context):
        # Establish connection to Redshift
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading data into {self.table_name} table from SQL")

        # If not in append mode, truncate the target table before loading data
        if not self.append_mode:
            self.log.info(f"Truncating {self.table_name} table before loading new data.")
            redshift_hook.run(f"TRUNCATE TABLE {self.table_name}")

        # Run the SQL query to load data
        self.log.info(f"Running SQL query: {self.sql_query}")
        redshift_hook.run(self.sql_query)

        self.log.info(f"Data loaded into {self.table_name} table successfully.")
