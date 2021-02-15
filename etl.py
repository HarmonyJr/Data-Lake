import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.functions import unix_timestamp, from_unixtime
from pyspark.sql.types import IntegerType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - get filepath to song data file
    """
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    """
    - read song data file
    """
    df = spark.read.json(song_data)
    
    """
    - extract columns to create songs table
    """
    songs_table = df.select(["song_id","title","artist_id","year","duration"]).distinct()
    
    """
    - write songs table to parquet files partitioned by year and artist
    """
    songs_table.write.partitionBy("year","artist_id").mode("overwrite").parquet(output_data + "song")

    """
    - extract columns to create artists table
    """
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).distinct()
    
    """
    - write artists table to parquet files
    """
    artists_table.write.mode("overwrite").parquet(output_data + "artist")


def process_log_data(spark, input_data, output_data):
    """
    - get filepath to log data file
    """
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    """
    - read log data file
    """    
    df = spark.read.json(log_data)
    
    """
    -  filter by actions for song plays
    """
    df = df.filter("page = 'NextSong'")

    """
    -  extract columns for users table 
    """ 
    users_table = df.selectExpr("userId as user_id", \
                                 "firstName as first_name", \
                                 "lastName as last_name", \
                                 "gender", \
                                 "level").distinct()
    
    """
    -  write users table to parquet files
    """ 
    users_table.write.mode("overwrite").parquet(output_data + "user")

    """
    - create timestamp column from original timestamp column
    """
    get_timestamp = udf(lambda x: x/1000, IntegerType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    """
    - create datetime column from original timestamp column
    """
    get_datetime = udf(lambda x: from_unixtime(x), TimestampType())
    df =  df.withColumn('datetime', from_unixtime('start_time'))
    
    """
    - extract columns to create time table
    """
    time_table = df.withColumn("year", year("datetime"))\
                   .withColumn("hour", hour("datetime"))\
                   .withColumn("month", month("datetime"))\
                   .withColumn("day", dayofmonth("datetime"))\
                   .withColumn("week", weekofyear("datetime"))\
                   .withColumn("weekday", dayofweek("datetime"))\
                   .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    
    """
    - write time table to parquet files partitioned by year and month
    """
    time_table = time_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "time")
    
    """
    - read in song data to use for songplays table
    """
    song_data_path = os.path.join("s3a://udacity-dend/", "song_data/*/*/*.json")
    song_df = spark.read.json(song_data_path)  
    
    """
    - create view for song table
    """
    song_df.createOrReplaceTempView("song")
    
    """
    - create view for log table
    """
    df.createOrReplaceTempView("log")
    
    """
    - extract columns from joined song and log datasets to create songplays table 
    """
    songplays_table = spark.sql("""
        SELECT DISTINCT a.datetime, a.userId as user_id, a.level, b.song_id, b.artist_id, a.sessionId as session_id, a.location,
                        a.userAgent as user_agent, year(a.datetime) as year, month(a.datetime) as month
        FROM log a 
        INNER JOIN song b
        ON a.song = b.title
        AND a.artist = b.artist_name
        AND a.length = b.duration
    """)  

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").mode("overwrite").parquet(output_data + "songplay")
    
                                      
def main():
    print("Main")
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-data-eng-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
