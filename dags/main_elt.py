from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('sparkify_elt',
          default_args=default_args,
          description='Load and transform sparkify data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
          schedule_interval="@hourly",
          max_active_runs=1,
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    create_table_sql=SqlQueries.create_staging_events,
    provide_context=True,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_settings="JSON 's3://udacity-dend/log_json_path.json'"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    create_table_sql=SqlQueries.create_staging_songs,
    provide_context=True,
    aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_settings="JSON 'auto'",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    #aws_credentials_id='aws_credentials',
    redshift_conn_id='redshift',
    table='songplays',
    create_table_sql=SqlQueries.create_songplays_table,
    table_insert_sql=SqlQueries.songplay_table_insert,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag, 
    redshift_conn_id='redshift',
    table='users',
    create_table_sql=SqlQueries.create_users_table,
    table_insert_sql=SqlQueries.user_table_insert,
    provide_context=True,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    create_table_sql=SqlQueries.create_songs_table,
    table_insert_sql=SqlQueries.song_table_insert,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    create_table_sql=SqlQueries.create_artists_table,
    table_insert_sql=SqlQueries.artist_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    create_table_sql=SqlQueries.create_time_table,
    table_insert_sql=SqlQueries.time_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    test_sql="SELECT COUNT(*) FROM songplays",
    test_answer=[(6820,)],

)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator