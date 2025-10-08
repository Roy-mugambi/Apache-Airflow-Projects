from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
import boto3
import logging

#Default Arguments Declaration

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


BUCKET_NAME = 'nl-aws-de-labs'
SONGS_FILE_PATH = 'spotify_data/songs.csv'
USERS_FILE_PATH = 'spotify_data/users.csv'
STREAMS_PREFIX = 'spotify_data/streams/'
ARCHIVE_PREFIX = 'spotify_data/streams/archived/'

REQUIRED_COLUMNS = {
    'songs': ['id', 'track_id', 'artists', 'album_name', 'track_name', 'popularity',
              'duration_ms', 'explicit', 'danceability', 'energy', 'key', 'loudness',
              'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness',
              'valence', 'tempo', 'time_signature', 'track_genre'],
    'streams': ['user_id', 'track_id', 'listen_time'],
    'users': ['user_id', 'user_name', 'user_age', 'user_country', 'created_at']
}

#Checking for CSV files in S3 Bucket
def list_s3_files(prefix, bucket=BUCKET_NAME):
    """List all files in S3 that match the prefix."""
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('.csv')]
    logging.info(f"Files found under {prefix}: {files}")
    return files

#Function for returning the body of the CSV file
def read_s3_csv(file_name, bucket=BUCKET_NAME):
    """Read a CSV file from S3 and return as DataFrame."""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=file_name)
    return pd.read_csv(obj['Body'])

# Dataset Validation
def validate_datasets():
    validation_results = {}

    # Validate Songs
    try:
        songs_data = read_s3_csv(SONGS_FILE_PATH)
        missing = set(REQUIRED_COLUMNS['songs']) - set(songs_data.columns)
        validation_results['songs'] = not missing
        if missing:
            logging.warning(f"Missing columns in songs.csv: {missing}")
    except Exception as e:
        validation_results['songs'] = False
        logging.error(f"Error reading songs: {e}")

    # Validate Users
    try:
        users_data = read_s3_csv(USERS_FILE_PATH)
        missing = set(REQUIRED_COLUMNS['users']) - set(users_data.columns)
        validation_results['users'] = not missing
        if missing:
            logging.warning(f"Missing columns in users.csv: {missing}")
    except Exception as e:
        validation_results['users'] = False
        logging.error(f"Error reading users: {e}")

    # Validate Streams
    try:
        stream_files = list_s3_files(STREAMS_PREFIX)
        for f in stream_files:
            df = read_s3_csv(f)
            missing = set(REQUIRED_COLUMNS['streams']) - set(df.columns)
            if missing:
                validation_results['streams'] = False
                logging.warning(f"Missing columns in {f}: {missing}")
                break
            validation_results['streams'] = True
    except Exception as e:
        validation_results['streams'] = False
        logging.error(f"Error reading streams: {e}")

    return validation_results

#Branching Logic Using Xcom
def branch_task(ti):
    results = ti.xcom_pull(task_ids='validate_datasets')
    if all(results.values()):
        return 'calculate_genre_level_kpis'
    else:
        return 'end_dag'

#Redshift Upsert command
def upsert_to_redshift(df, table_name, id_columns):
    """Upsert DataFrame into Redshift via PostgresHook."""
    redshift_hook = PostgresHook(postgres_conn_id='redshift_default')
    conn = redshift_hook.get_conn()
    cursor = conn.cursor()

    cols = ', '.join(df.columns)
    vals = ', '.join(['%s'] * len(df.columns))
    tmp_insert = f"INSERT INTO reporting_schema.tmp_{table_name} ({cols}) VALUES ({vals})"

    try:
        cursor.executemany(tmp_insert, [tuple(x) for x in df.to_numpy()])
        condition = ' AND '.join([f'tmp.{c}=main.{c}' for c in id_columns])
        merge_query = f"""
        BEGIN;
        DELETE FROM reporting_schema.{table_name} AS main
        USING reporting_schema.tmp_{table_name} AS tmp
        WHERE {condition};

        INSERT INTO reporting_schema.{table_name}
        SELECT * FROM reporting_schema.tmp_{table_name};

        TRUNCATE TABLE reporting_schema.tmp_{table_name};
        COMMIT;
        """
        cursor.execute(merge_query)
        conn.commit()
        logging.info(f"Data merged successfully into {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error upserting to Redshift: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

# Genre Level and Hourly KPI Calculations
def calculate_genre_level_kpis():
    stream_files = list_s3_files(STREAMS_PREFIX)
    streams_data = pd.concat([read_s3_csv(f) for f in stream_files], ignore_index=True)
    songs_data = read_s3_csv(SONGS_FILE_PATH)

    streams_data['listen_date'] = pd.to_datetime(streams_data['listen_time']).dt.date
    merged = streams_data.merge(songs_data, on='track_id', how='left')

    genre_counts = merged.groupby(['listen_date', 'track_genre']).size().reset_index(name='listen_count')
    merged['duration_seconds'] = merged['duration_ms'] / 1000
    avg_duration = merged.groupby(['listen_date', 'track_genre'])['duration_seconds'].mean().reset_index(name='average_duration')

    total_listens = merged.groupby('listen_date').size().reset_index(name='total_listens')
    genre_counts = genre_counts.merge(total_listens, on='listen_date')
    genre_counts['popularity_index'] = genre_counts['listen_count'] / genre_counts['total_listens']

    most_popular = merged.groupby(['listen_date', 'track_genre', 'track_id']).size().reset_index(name='count')
    most_popular = most_popular.sort_values(['listen_date', 'track_genre', 'count'], ascending=[True, True, False])
    most_popular = most_popular.drop_duplicates(['listen_date', 'track_genre']).rename(columns={'track_id': 'most_popular_track_id'})

    final_kpis = genre_counts.merge(avg_duration, on=['listen_date', 'track_genre'])
    final_kpis = final_kpis.merge(most_popular[['listen_date', 'track_genre', 'most_popular_track_id']], on=['listen_date', 'track_genre'])
    upsert_to_redshift(final_kpis, 'genre_level_kpis', ['listen_date', 'track_genre'])

def calculate_hourly_kpis():
    stream_files = list_s3_files(STREAMS_PREFIX)
    streams_data = pd.concat([read_s3_csv(f) for f in stream_files], ignore_index=True)
    songs_data = read_s3_csv(SONGS_FILE_PATH)
    users_data = read_s3_csv(USERS_FILE_PATH)

    streams_data['listen_time'] = pd.to_datetime(streams_data['listen_time'])
    streams_data['listen_date'] = streams_data['listen_time'].dt.date
    streams_data['listen_hour'] = streams_data['listen_time'].dt.hour

    merged = streams_data.merge(songs_data, on='track_id').merge(users_data, on='user_id')

    hourly_listeners = merged.groupby(['listen_date', 'listen_hour'])['user_id'].nunique().reset_index(name='unique_listeners')
    top_artist = merged.groupby(['listen_date', 'listen_hour', 'artists']).size().reset_index(name='listen_counts')
    top_artist = top_artist.loc[top_artist.groupby(['listen_date', 'listen_hour'])['listen_counts'].idxmax()].rename(columns={'artists': 'top_artist'})

    merged['session_id'] = merged['user_id'].astype(str) + '-' + merged['listen_time'].astype(str)
    avg_sessions = merged.groupby(['listen_date', 'listen_hour', 'user_id']).size().groupby(['listen_date', 'listen_hour']).mean().reset_index(name='avg_sessions_per_user')

    diversity = merged.groupby(['listen_date', 'listen_hour'])['track_id'].agg(['nunique', 'count']).reset_index()
    diversity['diversity_index'] = diversity['nunique'] / diversity['count']

    users_data['age_group'] = pd.cut(users_data['user_age'], bins=[0, 25, 35, 45, 55, 65, 100], labels=['18-25', '26-35', '36-45', '46-55', '56-65', '66+'])
    engagement = merged.merge(users_data[['user_id', 'age_group']], on='user_id').groupby(['listen_date', 'listen_hour', 'age_group']).size().reset_index(name='streams')
    top_group = engagement.loc[engagement.groupby(['listen_date', 'listen_hour'])['streams'].idxmax()].rename(columns={'age_group': 'most_engaged_age_group'})

    final = hourly_listeners.merge(top_artist, on=['listen_date', 'listen_hour'])
    final = final.merge(avg_sessions, on=['listen_date', 'listen_hour'])
    final = final.merge(diversity[['listen_date', 'listen_hour', 'diversity_index']], on=['listen_date', 'listen_hour'])
    final = final.merge(top_group[['listen_date', 'listen_hour', 'most_engaged_age_group']], on=['listen_date', 'listen_hour'])

    upsert_to_redshift(final, 'hourly_kpis', ['listen_date', 'listen_hour'])

#Archiving processed files into S3
def move_processed_files():
    s3 = boto3.client('s3')
    files = list_s3_files(STREAMS_PREFIX)
    for f in files:
        dest = f.replace(STREAMS_PREFIX, ARCHIVE_PREFIX)
        s3.copy_object(CopySource={'Bucket': BUCKET_NAME, 'Key': f}, Bucket=BUCKET_NAME, Key=dest)
        s3.delete_object(Bucket=BUCKET_NAME, Key=f)
        logging.info(f"Archived {f} to {dest}")

#DAG Definition
with DAG('data_validation_and_kpi_computation',
         default_args=default_args,
         description='Validate datasets and compute KPIs',
         schedule_interval='@daily',
         catchup=False) as dag:

    validate_datasets_task = PythonOperator(
        task_id='validate_datasets',
        python_callable=validate_datasets
    )

    check_validation = BranchPythonOperator(
        task_id='check_validation',
        python_callable=branch_task,
        provide_context=True
    )

    genre_kpis = PythonOperator(
        task_id='calculate_genre_level_kpis',
        python_callable=calculate_genre_level_kpis
    )

    hourly_kpis = PythonOperator(
        task_id='calculate_hourly_kpis',