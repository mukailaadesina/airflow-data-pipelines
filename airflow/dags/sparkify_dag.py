from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from sparkify_dimensions_subdag import load_dimensional_tables_dag



# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
#CreateTablesOperator

start_date = datetime.utcnow()

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'end_date': datetime(2019, 11, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
    'check_null_sql' : """SELECT COUNT(*) FROM {} WHERE {} IS NULL;""",
    'check_count_sql' : """SELECT COUNT(*) FROM {};"""
}

dag_name='sparkify_dag'
dag = DAG(dag_name,
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

#create_redshift_tables = CreateTablesOperator(
#    task_id='Create_tables',
#    dag=dag,
#    redshift_conn_id="redshift"
#)

#create_tables = PostgresOperator(
#    task_id="create_tables",
#    dag=dag,
#    postgres_conn_id="redshift",
#    sql="create_tables.sql"
#)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    #s3_key="log_json_path",
    region="us-west-2",
    file_format="FORMAT AS json 's3://udacity-dend/log_json_path.json'",
    #file_format=f"FORMAT AS json '{jsonpaths}'",
    execution_date=start_date
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    file_format="FORMAT AS json 'auto'",
    execution_date=start_date
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    table ="songplays",
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

load_users_dimension_table_task_id='Load_users_dim_table'
load_users_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_users_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2018, 11, 1),
        table="users",
        sql_query=SqlQueries.user_table_insert,
    ),    
    task_id=load_users_dimension_table_task_id,
    dag=dag
)
#load_user_dimension_table = LoadDimensionOperator(
#    task_id='Load_users_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    table="users",
#    query=SqlQueries.user_table_insert,
#    append=False
#)

load_songs_dimension_table_task_id='Load_songs_dim_table'
load_songs_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_songs_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        start_date= datetime(2018, 11, 1),
        table="songs",
        sql_query=SqlQueries.song_table_insert,
    ),
    task_id=load_songs_dimension_table_task_id,
    dag=dag,
)

#load_song_dimension_table = LoadDimensionOperator(
#    task_id='Load_songs_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    table="songs",
#    query=SqlQueries.song_table_insert,
#    append=False
#)

load_artists_dimension_table_task_id='Load_artists_dim_table'
load_artists_dimension_table = SubDagOperator(
      subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_artists_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="artists",
        start_date= datetime(2018, 11, 1),
        sql_query=SqlQueries.artist_table_insert,
    ),
    task_id=load_artists_dimension_table_task_id,
    dag=dag,
)

#load_artist_dimension_table = LoadDimensionOperator(
#    task_id='Load_artists_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    table="artists",
#    query=SqlQueries.artist_table_insert,
#    append=False
#)

load_time_dimension_table_task_id='Load_time_dim_table'
load_time_dimension_table = SubDagOperator(
    subdag=load_dimensional_tables_dag(
        parent_dag_name=dag_name,
        task_id=load_time_dimension_table_task_id,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="time",
        start_date= datetime(2018, 11, 1),
        sql_query=SqlQueries.time_table_insert,
    ),
    task_id=load_time_dimension_table_task_id,
    dag=dag,
)

#load_time_dimension_table = LoadDimensionOperator(
#    task_id='Load_time_dim_table',
#    dag=dag,
#    redshift_conn_id="redshift",
#    table="time",
#    query=SqlQueries.time_table_insert,
#    append=False
#)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    check_null_sql=default_args['check_null_sql'],
    check_count_sql=default_args['check_count_sql'],
    tables=['songplays', 'songs', 'users', 'artists', 'time'],
    columns=['playid', 'songid', 'userid', 'artistid', 'start_time']
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting tasks dependencies
# create_redshift_tables 

start_operator >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_users_dimension_table, load_songs_dimension_table, load_artists_dimension_table,
                           load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
