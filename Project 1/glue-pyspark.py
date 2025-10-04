from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, sum as _sum, avg, approx_count_distinct, expr, rank, desc
from pyspark.sql.window import Window
import os

spark = SparkSession.builder.appName("Spotify Advanced KPI Processing").getOrCreate()

# Fetching environment variables (can be set in Glue job or as defaults in Airflow)
bucket_name = 'roy-nl-airflow-dags'
base_path = 's3://roy-nl-airflow-dags/spotify_data'
songs_file_path = f'{base_path}/songs/'
users_file_path = f'{base_path}/users/'
user_streams_path = f'{base_path}/streams/*'
output_path = f'{base_path}/processed/music_kpis/'

# Load data
songs_df = spark.read.csv(songs_file_path, header=True, inferSchema=True).dropna(subset=["track_id"])
users_df = spark.read.csv(users_file_path, header=True, inferSchema=True)
user_streams_df = spark.read.csv(user_streams_path + "*", header=True, inferSchema=True).dropna(subset=["track_id"])
user_streams_df = user_streams_df.withColumn("report_date", to_date(col("listen_time")))

# KPIs per song per day
song_kpis_df = user_streams_df.groupBy("track_id", "report_date").agg(
    count("*").alias("total_listens"),
    approx_count_distinct("user_id").alias("unique_users"),
    _sum(expr("unix_timestamp(listen_time)")).alias("total_listening_time"),
    avg(expr("unix_timestamp(listen_time)")).alias("avg_listening_time_per_user")
)

# Join with songs
song_kpis_with_details_df = song_kpis_df.join(songs_df, "track_id")

# Ranking and top songs
windowSpec = Window.partitionBy("report_date", "track_genre").orderBy(desc("total_listens"))
ranked_songs_df = song_kpis_with_details_df.withColumn("rank", rank().over(windowSpec))
top_songs_per_genre = ranked_songs_df.filter(col("rank") <= 3)

# Top genres
genre_window = Window.partitionBy("report_date").orderBy(desc("total_listens"))
top_genres = top_songs_per_genre.withColumn("genre_rank", rank().over(genre_window)).filter(col("genre_rank") <= 5)

final_df = top_genres.select("report_date", "track_id", "track_name", "artists", "track_genre",
                             "total_listens", "unique_users", "total_listening_time", "avg_listening_time_per_user")

# Write to S3
final_df.write.mode("overwrite").csv(output_path, header=True)
spark.stop()
