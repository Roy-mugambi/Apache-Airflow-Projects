from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import boto3
import logging
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15)
}

dag = DAG('process-songs-metrics',
          default_args=default_args,
          description='Trigger Glue job when new files are uploaded to S3 and manage output',
          schedule_interval='*/15 * * * *',
          catchup=False)

#Variables Declaration
bucket_name = 'roy-nl-airflow-dags'
base_path = 'spotify_data' 

user_streams_prefix = f'{base_path}/streams/'
songs_prefix = f'{base_path}/songs/'
users_prefix = f'{base_path}/users/'
archived_prefix = f'{base_path}/processed/archived_streams/'

region = 'us-east-1'
spark_job_name = 'music_kpi_etl'
python_job_name = 'upsert_dynamo_music_kpi'

def check_files_in_s3(prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    contents = response.get('Contents', [])
    for obj in contents:
        if obj['Size'] > 0:
            return True
    return False

def check_all_files(**kwargs):
    if (check_files_in_s3(user_streams_prefix)
            and check_files_in_s3(songs_prefix)
            and check_files_in_s3(users_prefix)):
        return 'trigger_spark_job_task'
    return 'skip_execution'

def wait_for_glue_job_completion(job_name, client, poll_interval=60):
    while True:
        response = client.get_job_runs(JobName=job_name, MaxResults=1)
        job_runs = response.get('JobRuns', [])
        
        if job_runs and job_runs[0]['JobRunState'] in ['RUNNING', 'STARTING', 'STOPPING']:
            logging.info(f"Glue job {job_name} is still running. Waiting for it to finish...")
            time.sleep(poll_interval)
        else:
            logging.info(f"Glue job {job_name} has finished.")
            break

def trigger_glue_job(job_name, **kwargs):
    client = boto3.client('glue', region_name=region)
    wait_for_glue_job_completion(job_name, client)
    client.start_job_run(JobName=job_name)

def wait_for_spark_job_completion(**kwargs):
    client = boto3.client('glue', region_name=region)
    wait_for_glue_job_completion(spark_job_name, client)


def wait_for_python_job_completion(**kwargs):
    client = boto3.client('glue', region_name=region)
    wait_for_glue_job_completion(python_job_name, client)

def move_files_to_archived(**kwargs):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=user_streams_prefix)
    for obj in response.get('Contents', []):
        source_key = obj['Key']
        dest_key = source_key.replace(user_streams_prefix, archived_prefix)
        s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': source_key}, Key=dest_key)
        s3.delete_object(Bucket=bucket_name, Key=source_key)

# Tasks
check_files = BranchPythonOperator(
    task_id='check_files',
    python_callable=check_all_files,
    dag=dag
)

trigger_spark_job_task = PythonOperator(
    task_id='trigger_spark_job_task',
    python_callable=trigger_glue_job,
    op_args=[spark_job_name],
    dag=dag
)

wait_for_spark_job_completion_task = PythonOperator(
    task_id='wait_for_spark_job_completion_task',
    python_callable=wait_for_spark_job_completion,
    dag=dag
)

trigger_python_job_task = PythonOperator(
    task_id='trigger_python_job_task',
    python_callable=trigger_glue_job,
    op_args=[python_job_name],
    dag=dag
)

wait_for_python_job_completion_task = PythonOperator(
    task_id='wait_for_python_job_completion_task',
    python_callable=wait_for_python_job_completion,
    dag=dag
)

move_files = PythonOperator(
    task_id='move_files',
    python_callable=move_files_to_archived,
    dag=dag
)

skip_execution = DummyOperator(task_id='skip_execution', dag=dag)

# Dependencies
check_files >> [trigger_spark_job_task, skip_execution]
trigger_spark_job_task >> wait_for_spark_job_completion_task >> trigger_python_job_task >> wait_for_python_job_completion_task >> move_files
