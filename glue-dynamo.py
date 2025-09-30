import boto3
import csv
from io import StringIO
from decimal import Decimal
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
bucket_name = 'roy-nl-airflow-dags'
base_path = 's3://roy-nl-airflow-dags/spotify_data'
output_prefix = f'{base_path}/processed/music_kpis/'

# AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("music_stream_kpis")

# Get latest CSV
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=output_prefix)
latest_file = max(response['Contents'], key=lambda x: x['LastModified'])['Key']
csv_obj = s3.get_object(Bucket=bucket_name, Key=latest_file)
csv_data = StringIO(csv_obj['Body'].read().decode('utf-8'))

reader = csv.DictReader(csv_data)

# Upsert each row
for row in reader:
    try:
        table.update_item(
            Key={'track_id': row['track_id'], 'report_date': row['report_date']},
            UpdateExpression='SET total_listens = :tl, unique_users = :uu, total_listening_time = :tlt, avg_listening_time_per_user = :alt',
            ExpressionAttributeValues={
                ':tl': int(row['total_listens']),
                ':uu': int(row['unique_users']),
                ':tlt': Decimal(str(row['total_listening_time'])),
                ':alt': Decimal(str(row['avg_listening_time_per_user']))
            }
        )
        logging.info(f"Upserted {row['track_id']} for {row['report_date']}")
    except Exception as e:
        logging.error(f"Failed to upsert row {row}: {e}")
