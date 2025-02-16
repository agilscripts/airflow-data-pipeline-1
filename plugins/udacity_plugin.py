from airflow.plugins_manager import AirflowPlugin

# Relative imports (the dot `.` means "this same package/folder")
from .operators.stage_redshift import StageToRedshiftOperator
from .operators.load_fact import LoadFactOperator
from .operators.load_dimension import LoadDimensionOperator
from .operators.data_quality import DataQualityOperator

from .udacity_helpers.sql_queries import SqlQueries


class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"

    operators = [
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator
    ]
    helpers = [
        SqlQueries
    ]
