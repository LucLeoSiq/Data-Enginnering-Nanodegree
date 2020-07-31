import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a Apache Spark session to process the data.
    """
    spark = SparkSession.builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Transforms song data from S3 to analytics tables on S3
    
    Function reads in song data in from S3 (JSON); defines the schema of 
    songs and artists analytics tables; processes the raw data into those 
    tables; and then writes the tables into partitioned parquet files on S3.
    
    Arguments:
        - spark: Spark session
        - input_data: S3 bucket where data is read from
        - output_data: S3 bucket where data is written to
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df = spark.read.format("json").load(song_data)

    # convert year to integer type
    df = df.withColumn("year", df["year"].cast(IntegerType()))

    # create schema-on-read table for songs
    df.createOrReplaceTempView("songs")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT DISTINCT 
                                            song_id, 
                                            LTRIM(RTRIM(title)) AS title, 
                                            artist_id,
                                            IF(year=0,null,year) AS year, 
                                            duration
                            FROM songs
                            """)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs_table")

    # extract columns to create artists table
    artists_table = spark.sql("""
                             SELECT DISTINCT
                                             artist_id,
                                             artist_name,
                                             IF(artist_location='' OR artist_location='None',null,artist_location) AS artist_location,
                                             artist_latitude,
                                             artist_longitude
                             FROM songs
                             """)

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists_table")


def process_log_data(spark, input_data, output_data):
    """
    Reads data file containing the logs from user activity and loads into parquet files for time_table, 
    users_table and songplays_table in S3.
    
    Arguments:
        - spark: spark session
        - input_data: path to input_data to be processed (log_data)
        - output_data: path to location to store the output (parquet files).
    """

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.format("json").load(log_data)

    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # create timestamp column f
    from pyspark.sql.types import TimestampType

    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('start_time', get_timestamp('ts'))

    # create schema-on-read table for loag data
    df.createOrReplaceTempView("log_data")

    # extract columns for users table
    users_table = spark.sql("""
                            SELECT lg.userid, lg.firstname, lg.lastname, lg.gender, lg.level
                            FROM ( SELECT start_time, userid, firstname, lastname, gender,level,
                                   RANK() OVER (PARTITION BY userid ORDER BY start_time DESC) AS rank
                                   FROM log_data
                                  ) AS lg
                            WHERE qry.rank = 1
                            """)

    # write users table to parquet files
    users_table.write.parquet(output_data + "users_table")

    # extract columns to create time table
    time_table = df.select("start_time",
                           hour("start_time").alias('hour'),
                           dayofmonth("start_time").alias('day'),
                           weekofyear("start_time").alias('week'),
                           month("start_time").alias('month'),
                           year("start_time").alias('year'),
                           date_format("start_time","u").alias('weekday')).distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time_table")

    # read in song, artist and time data for songplays table
    song_df = spark.read.parquet("s3a://dend-udacity-p4/songs_table")
    song_df.createOrReplaceTempView("songs_table")

    artist_df = spark.read.parquet("s3a://dend-udacity-p4/artists_table")
    artist_df.createOrReplaceTempView("artists_table")

    time_table.createOrReplaceTempView("time_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql("""
                                SELECT ld.start_time,
                                       ld.userid,
                                       ld.level,
                                       tt.year,
                                       tt.month,
                                       q.song_id,
                                       q.artist_id,
                                       ld.sessionid,
                                       ld.location,
                                       ld.useragent
                                  FROM log_data ld
                                  JOIN time_table tt ON (ld.start_time = tt.start_time)
                                  LEFT JOIN (
                                           SELECT st.song_id,
                                                  st.title,
                                                  a.artist_id,
                                                  a.artist_name
                                             FROM songs_table st
                                             JOIN artists_table a ON (st.artist_id = a.artist_id)
                                          ) AS q ON (ld.song = q.title AND ld.artist = q.artist_name)
                               """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + "songplays_table")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-udacity-p4/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()