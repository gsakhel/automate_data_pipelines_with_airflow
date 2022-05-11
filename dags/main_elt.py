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

dag = DAG('sparkify_etl',
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
    append_only=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    create_table_sql=SqlQueries.create_songs_table,
    table_insert_sql=SqlQueries.song_table_insert,
    append_only=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    create_table_sql=SqlQueries.create_artists_table,
    table_insert_sql=SqlQueries.artist_table_insert,
    append_only=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    create_table_sql=SqlQueries.create_time_table,
    table_insert_sql=SqlQueries.time_table_insert,
    append_only=False,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    checks=[
        {'test_sql': 'SELECT COUNT(*) FROM songplays',
         'expected_result': [(6820,)]
         },
        {'test_sql': "SELECT name FROM artists WHERE artistid='ARKYKXP11F50C47A6A'",
         'expected_result': [('The Supersuckers',)]
         },
        {'test_sql': "SELECT userid, level, sessionid FROM songplays WHERE playid='d2ecd4f950f2d064e84a57e38f625c6f'",
         'expected_result': [(8,'free',139)]},
        {'test_sql': "SELECT title, year FROM songs WHERE artistid='ARSW5F51187FB4CFC9' ORDER BY year, title ASC",
         'expected_result': [('Brother',1992), ('God Smack', 1992), ('Lesson Learned', 2009)]
         }
    ],
    comparison_checks=[
        {'test_sql': 'SELECT COUNT(*) FROM songs',
         'operator': '>',
         'inequality': 0
         },
        {'test_sql': "SELECT name FROM artists WHERE artistid='ARVNNXD1187B9AE50D'",
         'operator': '=',
         'inequality': 'Marvin Gaye'
         },
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Process Flow
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