import configparser
import datetime
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config.get('CREDENTIALS', 'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('CREDENTIALS', 'AWS_SECRET_ACCESS_KEY')


def create_spark_session():
    """
    Initialize spark session
    :return: a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data, artist data from s3 bucket, normalize the data and save
    as parquet files to another s3 bucket
    :param spark: a spark session
    :param input_data: input date for songs dataset
    :param output_data: output data for parquet dataset
    :return: None
    """
    # get filepath to song data file
    song_data = input_data + "data/song_data/song_data/*/*/*/*.json"

    # setting schema types for song data fields
    song_data_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("year", StringType(), True),
        StructField("duration", DoubleType(), True),
        StructField("artist_id", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_location", StringType(), True),
        StructField("artist_latitude", DoubleType(), True),
        StructField("artist_longitude", DoubleType(), True),
    ])

    # read song data file
    song_df = spark.read.json(song_data, schema=song_data_schema)

    # columns to be used
    artist_id = "artist_id"
    artist_latitude = "artist_latitude"
    artist_location = "artist_location"
    artist_longitude = "artist_longitude"
    artist_name = "artist_name"
    duration = "duration"
    num_songs = "num_songs"
    song_id = "song_id"
    title = "title"
    year = "year"

    # extract columns to create songs table
    songs_table = song_df.select(song_id, title, artist_id, year, duration)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(f"{output_data}songs_table.parquet")

    # extract columns to create artists table
    artists_table = song_df.select(artist_id, artist_name, artist_location, artist_latitude, artist_longitude, num_songs)

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Process log data in s3 bucket by extracting users table, time table
    and songplays table. Normalize and transform data to be store as
    parquet files in s3 bucket
    :param spark: a spark session
    :param input_data: data from s3 bucket location
    :param output_data: data to s3 bucket location
    :return: None
    """
    # get filepath to log data file
    log_data = input_data + "data/log_data/*.json"

    # setting schema types for log data fields
    log_data_schema = StructType([
        StructField("artist", StringType(), True),
        StructField("auth", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("itemInSession", LongType(), True),
        StructField("lastName", StringType(), True),
        StructField("length", DoubleType(), True),
        StructField("level", StringType(), True),
        StructField("location", StringType(), True),
        StructField("method", StringType(), True),
        StructField("page", StringType(), True),
        StructField("registration", DoubleType(), True),
        StructField("sessionId", LongType(), True),
        StructField("song", StringType(), True),
        StructField("status", LongType(), True),
        StructField("ts", LongType(), True),
        StructField("userAgent", StringType(), True),
        StructField("userId", StringType(), True),
    ])

    # read log data file
    log_df = spark.read.json(log_data, schema=log_data_schema)

    # columns to be used
    firstName = 'firstName'
    gender = 'gender'
    lastName = 'lastName'
    level = 'level'
    userId = 'userId'
    start_time = 'start_time'
    hour = 'hour'
    day = 'day'
    week = 'week'
    month = 'month'
    year = 'year'
    weekday = 'weekday'

    # filter by actions for song plays
    log_df = log_df.filter(log_df.page == 'NextSong')

    # extract columns for users table
    users_table = log_df.select(firstName, lastName, gender, level, userId)

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000), TimestampType())
    log_df = log_df.withColumn("timestamp", get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: F.to_date(x), TimestampType())
    log_df = log_df.withColumn("start_time", get_datetime(log_df.ts))

    # extract columns to create time table
    log_df = log_df.withColumn("hour", F.hour("timestamp"))
    log_df = log_df.withColumn("day", F.dayofweek("timestamp"))
    log_df = log_df.withColumn("week", F.weekofyear("timestamp"))
    log_df = log_df.withColumn("month", F.month("timestamp"))
    log_df = log_df.withColumn("year", F.year("timestamp"))
    log_df = log_df.withColumn("weekday", F.dayofweek("timestamp"))

    time_table = log_df.select(start_time, hour, day, week, month, year, weekday)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f"{output_data}time_table.parquet")

    # read in song data to use for songplays table
    song_data = input_data + "data/song_data/song_data/*/*/*/*.json"
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    time_table.createOrReplaceTempView("time_table")
    log_df.createOrReplaceTempView("log_df_table")
    song_df.createOrReplaceTempView("song_df_table")
    songplays_table = spark.sql("""
       SELECT DISTINCT lt.start_time,
                        lt.userId,
                        lt.level,
                        lt.sessionId,
                        lt.location,
                        lt.userAgent,
                        st.song_id,
                        st.artist_id,
                        tt.year,
                        tt.month
        FROM log_df_table lt 
        INNER JOIN song_df_table st 
        ON st.artist_name = lt.artist 
        INNER JOIN time_table tt
        ON tt.start_time = lt.start_time 
    """)

    # create songplays_id columns which increases monotonically
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(f"{output_data}songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
