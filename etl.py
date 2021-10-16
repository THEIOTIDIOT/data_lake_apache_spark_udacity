import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import IntegerType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ Creates a spark session configured to be executed on AWS EMR Cluster"""
    spark = SparkSession \
        .builder \
        .appName("Sparkify Data Lake - ETL Pipeline")\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")\
        .getOrCreate()
    return spark

def extract_song_data(spark, input_data):
    """ JSON files containing song data located in a root directory specified by the input data path
    Args:
    spark - Spark session created for the AWS EMR Cluster
    input_data - Path used to indicate the root directory of the JSON data files
    Returns:
    DataFrame object containing the songs data
    """
    
    # get filepath to song data file
    song_data_path = input_data + "song_data/*/*/*/*.json"
    
    # read song data files
    df = spark.read.format("json")\
    .option("recursiveFileLookup", "true")\
    .load(song_data_path)
    return df
    
def process_song_data(spark, input_data, output_data):
    """ Extracts the song data located in the input data directory, transforms these data into dimension tables and loads the new tables as partitioned parquet files to the outputs_data directory
    
    Args: 
    spark - Spark session created for the AWS EMR Cluster
    input_data - Path used to indicate the root directory of the JSON data files
    output_data - Path used to indicate the root directory of the where the partitioned parquet files will be written to
    
    Returns:
    None
    """
    
    # read song data files
    df = extract_song_data(spark, input_data)

    # extract columns to create songs table
    # Columns : song_id, title, artist_id, year, duration
    songs_table = df.select("song_id", "title","artist_id", "year", "duration")
    
    # list of partitions
    partitionList = list(['year', 'artist_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(os.path.join(output_data, 'parquet_data/songs'), "overwrite", partitionList)

    # extract columns to create artists table
    # Columns : artist_id, name, location, lattitude, longitude
    artists_table = df.select("artist_id", df["artist_name"].alias("name"), df["artist_location"].alias("location"), df["artist_latitude"].alias("latitude"), df["artist_longitude"].alias("longitude"))
    
    # drop duplicate records
    artists_table = artists_table.dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'parquet_data/artists'), "overwrite")


def process_log_data(spark, input_data, output_data):
    """ Extracts the log data located in the input data directory, transforms these data into dimension tables and a fact table and loads the new tables as partitioned parquet files to the outputs_data directory
    
    Args: 
    spark - Spark session created for the AWS EMR Cluster
    input_data - Path used to indicate the root directory of the JSON data files
    output_data - Path used to indicate the root directory of the where the partitioned parquet files will be written to
    
    Returns:
    None
    """
    # get filepath to log data file
    log_data = input_data + "log-data/"

    # get log data
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    # user_id, first_name, last_name, gender, level
    users_table = df.select(df.userId.cast("integer").alias("user_id"), df.firstName.alias("first_name"),df.lastName.alias("last_name"),df.gender,df.level)
    
    # drop duplicate records
    users_table = users_table.dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data, "parquet_data/users"), "overwrite")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts : datetime.fromtimestamp(ts/1000.0) if type(ts) is int or type(ts) is float else None, TimestampType())
    
    df = df.withColumn('start_time', get_datetime(df.ts))
    df = df.withColumn('hour', hour(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('year', year(df.start_time))
    df = df.withColumn('weekday', dayofweek(df.start_time))
    df = df.withColumn('weekend', (df.weekday == 7) | (df.weekday == 1))

    # extract columns to create time table
    # columns : start_time, hour, day, week, month, year, weekend
    fields = ["start_time", "hour", "day", "week", "month", "year", "weekend"]
    exprs = ["{} as {}".format(field,field) for field in fields]
    time_table = df.selectExpr(*exprs)
    
    # drop duplicate records
    time_table = time_table.dropDuplicates()
    
    # partition list
    time_partition_list = ['year', 'month']
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "parquet_data/time"), "overwrite", time_partition_list)

    # read song data files
    songDf = extract_song_data(spark, input_data)
    songDf = songDf.drop('year')

    # extract columns from joined song and log datasets to create songplays table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    join_cond = [df.song == songDf.title, df.artist == songDf.artist_name, df.length == songDf.duration]
    songsLogsJoinTable = df.join(songDf, join_cond, 'full')
    songplaysUnclean = songsLogsJoinTable.withColumn("songplay_id", monotonically_increasing_id())
    songplays_table = songplaysUnclean.select(songplaysUnclean.songplay_id, songplaysUnclean.start_time,\
                                              songplaysUnclean.userId.alias("user_id"), songplaysUnclean.level,\
                                              songplaysUnclean.song_id, songplaysUnclean.artist_id,\
                                              songplaysUnclean.itemInSession.alias("session_id"), songplaysUnclean.location,\
                                              songplaysUnclean.userAgent.alias("user_agent"), songplaysUnclean.year,\
                                              songplaysUnclean.month)

    # partition list
    songplays_partition_list = ['year', 'month']

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(os.path.join(output_data, "parquet_data/songplays"), "overwrite", songplays_partition_list)

def main():
    """Runs the ETL pipeline used to generate a data lake for Sparkify"""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://benzbucket/data-lake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
