#!/usr/bin/python
# -*- coding: utf-8 -*-
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime, year, month, \
    dayofmonth, dayofweek, dayofyear, hour, weekofyear, date_format, \
    monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, \
    DoubleType as Dbl, StringType as Str, IntegerType as Int, \
    DateType as Dat, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create/get spark session 
    """

    spark = SparkSession.builder.config('spark.jars.packages',
            'org.apache.hadoop:hadoop-aws:2.7.0'
            ).getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Loads and process 'song_data' from s3, then process it and load it to output data path (locally or an s3 instance)
    """

    # get filepath to song data file

    song_data = input_data + 'song_data/*/*/*/*.json'

    songSchema = R([
        Fld('song_id', Str()),
        Fld('artist_id', Str()),
        Fld('artist_latitude', Dbl()),
        Fld('artist_location', Str()),
        Fld('artist_longitude', Dbl()),
        Fld('artist_name', Str()),
        Fld('duration', Dbl()),
        Fld('num_songs', Int()),
        Fld('title', Str()),
        Fld('year', Int()),
        ])

    # read song data file

    df = spark.read.json(song_data, schema=songSchema)

    # extract columns to create songs table

    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'
                            ]).dropDuplicates()

    # write songs table to parquet files partitioned by year and artist

    songs_table.write.partitionBy('year', 'artist_id'
                                  ).parquet(output_data + 'song_table/', mode='overwrite')

    # extract columns to create artists table

    artists_fields = ['artist_id', 'artist_name',
                      'artist_location as location',
                      'artist_latitude as latitude',
                      'artist_longitude as longitude']
    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files

    artists_table.write.parquet(output_data + 'artist_table/', mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Loads and process 'log_data' from s3, then process it and load it to output data path (locally or an s3 instance)
    """

    # get filepath to log data file

    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file

    df = spark.read.json(log_data)

    # filter by actions for song plays

    df = df.filter(df.page == 'NextSong')

    # extract columns for users table

    users_table = df.selectExpr(['userId as user_id',
                                'firstName as first_name',
                                'lastName as last_name', 'gender',
                                'level']).dropDuplicates()

    # write users table to parquet files
    users_table.write.parquet(output_data + 'user_table/', mode='overwrite')

    # create timestamp column from original timestamp column

    df = df.withColumn('start_time', from_unixtime(col('ts') / 1000))

    # extract columns to create time table

    time_table = df.select('ts', 'start_time').withColumn('year',
            year('start_time')).withColumn('month', month('start_time'
            )).withColumn('week', weekofyear('start_time'
                          )).withColumn('weekday',
            dayofweek('start_time')).withColumn('day',
            dayofyear('start_time')).withColumn('hour',
            hour('start_time')).dropDuplicates()

    # write time table to parquet files partitioned by year and month

    time_table.write.partitionBy("year", "month").parquet(output_data + 'time_table/', mode='overwrite')

    # read in song data to use for songplays table
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_dataset = spark.read.json(song_data)
    
    song_dataset.createOrReplaceTempView('song_dataset')
    df.createOrReplaceTempView('log_dataset')
    
    # extract columns from joined song and log datasets to create songplays table

    songplays_table = spark.sql("""SELECT DISTINCT
        l.start_time as start_time,
        YEAR(L.start_time) as year,
        MONTH(l.start_time) as month,
        l.userId as user_id,
        l.level as level,
        s.song_id as song_id,
        s.artist_id as artist_id,
        l.sessionId as session_id,
        l.location as location,
        l.userAgent as user_agent
        FROM song_dataset s
        JOIN log_dataset l
        ON s.artist_name = l.artist
        AND s.title = l.song
        AND s.duration = l.length
        """).dropDuplicates()
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'songplays/', mode='overwrite')

def main():
    spark = create_spark_session()

    input_data = config['FILES']['INPUT_DATA']
    output_data = config['FILES']['OUTPUT_DATA']
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == '__main__':
    main()
