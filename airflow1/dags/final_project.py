from datetime import datetime, timedelta
import pendulum
import os
import sys

plugins_path = "/Users/audrey/IdeaProjects/cd12380-data-pipelines-with-airflow-11/plugins"
if plugins_path not in sys.path:
    sys.path.append(plugins_path)

from plugins.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)
from plugins.udacity_helpers import SqlQueries

from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    # Start of the DAG
    start_operator = DummyOperator(task_id='Begin_execution')

    # Staging tasks
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
    )
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
    )

    # Fact table load
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    # Dimension table loads
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    # Data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    # (Optional) End of the DAG
    end_operator = DummyOperator(task_id='Stop_execution')

    # Setting up Dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                             load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table,
     load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
