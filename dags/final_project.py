from datetime import timedelta
import pendulum
import os
import sys
# Ensure Airflow finds the plugins folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../plugins")))

# Import operators from plugins folder
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.udacity_helpers.sql_queries import SqlQueries

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *'
)
def final_project():
    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_credentials",
        s3_bucket="your-bucket-name",
        s3_key="log-data",
        table_name="staging_events"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift_default",
        aws_conn_id="aws_credentials",
        s3_bucket="your-bucket-name",
        s3_key="song-data",
        table_name="staging_songs"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift_default",
        sql_query=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift_default",
        table_name="users",
        sql_query=SqlQueries.user_table_insert,
        append_mode=False
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift_default",
        table_name="songs",
        sql_query=SqlQueries.song_table_insert,
        append_mode=False
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift_default",
        table_name="artists",
        sql_query=SqlQueries.artist_table_insert,
        append_mode=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift_default",
        table_name="time",
        sql_query=SqlQueries.time_table_insert,
        append_mode=False
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift_default",
        sql="SELECT COUNT(*) FROM songplays",
        expected_result=1
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table,
                             load_artist_dimension_table, load_time_dimension_table]
    [load_user_dimension_table, load_song_dimension_table,
     load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()
