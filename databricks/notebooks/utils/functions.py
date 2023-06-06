# Databricks notebook source
from pyspark.sql.functions import from_unixtime
from pyspark.sql.types import TimestampType, BooleanType
from pyspark.sql.functions import col, lag, when, datediff, count
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

class Functions():
    
    @staticmethod
    def import_data(db):
        # '''
        # import data from csv file in storage into a df
        # '''
        print('start import_data')
        storage_account_name = 'rockstars'
        storage_account_access_key = 'a29hrPDqbA3VbXr8teiBxM4ODCNFK2k7KEctyg34xGWIRa6ST3oj1pyTb4vtFfaTlWAAktsFQHgn+AStBpJ4FA=='
        blob_container = 'rocksongs'
        spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
        filePath = "wasbs://" + blob_container + "@" + storage_account_name + ".blob.core.windows.net/raw_data_csv/raw_input_data.csv"
        df = spark.read.format("csv").load(filePath, inferSchema = True, header = True)

        '''
        transform time column values to datetime 
        and First column values to boolean
        then save as table
        '''
        from pyspark.sql.functions import from_unixtime
        from pyspark.sql.types import TimestampType, BooleanType
        df = df.select(col("RAW_SONG"),col("RAW_ARTIST"),col("CALLSIGN"),from_unixtime(df.TIME.cast("bigint")).cast(TimestampType()).alias("TIME"),col("UNIQUE_ID"),col("COMBINED"),df.First.cast("int").cast(BooleanType()))
        df.write.mode("overwrite").saveAsTable(f"{db}.raw_songs_data")

    @staticmethod
    def cleanup_data(db):
        print('start cleanup_data')
        data = spark.sql(f'''
                         SELECT * from {db}.raw_songs_data
                         ''')
        # Convert the 'RAW_ARTIST' column to lowercase
        data = data.withColumn('RAW_ARTIST', F.lower(F.col('RAW_ARTIST')))

        # Remove leading and trailing whitespace
        data = data.withColumn('RAW_ARTIST', F.trim(F.col('RAW_ARTIST')))

        # Remove punctuation marks using a regular expression
        data = data.withColumn('RAW_ARTIST', F.regexp_replace(F.col('RAW_ARTIST'), '[^\w\s]', ''))

        # write df as table
        data.write.mode("overwrite").saveAsTable(f"{db}.clean_songs_data")
    

    @staticmethod
    def calculate_times_played(db):
        print('start cleanupcalculate_times_played_data')
        # '''
        # Calculate the number of times a song has been played
        # '''
        data = spark.sql(f'''
                            select distinct(data.RAW_SONG), data.RAW_ARTIST,count.times_played from {db}.clean_songs_data data
                            inner join (
                                select RAW_SONG, count(RAW_SONG) as times_played from {db}.clean_songs_data
                                group by RAW_SONG) count
                            on count.RAW_SONG = data.RAW_SONG
                         ''')
        data.write.mode("overwrite").saveAsTable(f"{db}.songs_with_counts")

    @staticmethod
    def generate_top_10(db):
        print('start generate_top_10')
        # '''
        # generate the top 10 table and store as csv in storage
        # '''
        data = spark.sql(f'''
                         select * from {db}.songs_with_counts
                         order by times_played desc
                         limit 10
                         ''')
        data.write.mode("overwrite").saveAsTable(f"{db}.top_10")
        storage_account_name = 'rockstars'
        storage_account_access_key = 'a29hrPDqbA3VbXr8teiBxM4ODCNFK2k7KEctyg34xGWIRa6ST3oj1pyTb4vtFfaTlWAAktsFQHgn+AStBpJ4FA=='
        blob_container = 'rocksongs'
        spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
        filePath = "wasbs://" + self.blob_container + "@" + self.storage_account_name + ".blob.core.windows.net/lists/top10.csv"
        data.write.format('csv').mode('overwrite').option('header', 'true').save(filePath)

    @staticmethod
    def generate_bottem_10(db):
        print('start generate_bottem_10')
        # '''
        # generate the top 10 table and store as csv in storage
        # '''
        data = spark.sql(f'''
                         select * from {db}.songs_with_counts
                         order by times_played asc
                         limit 10
                         ''')
        data.write.mode("overwrite").saveAsTable(f"{db}.bottem_10")
        storage_account_name = 'rockstars'
        storage_account_access_key = 'a29hrPDqbA3VbXr8teiBxM4ODCNFK2k7KEctyg34xGWIRa6ST3oj1pyTb4vtFfaTlWAAktsFQHgn+AStBpJ4FA=='
        blob_container = 'rocksongs'
        spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
        filePath = "wasbs://" + self.blob_container + "@" + self.storage_account_name + ".blob.core.windows.net/lists/bottem10.csv"
        data.write.format('csv').mode('overwrite').option('header', 'true').save(filePath)

    @staticmethod
    def calculate_durations(db):
        # '''
        # Calculate the duration of each song by comparing the start time with the next start time
        # if there are multiple songs with same start time (not duplicates) then we devide the difference by the amount of songs with the same start time
        # '''
        data = spark.sql(f'''
                         SELECT * from {db}.clean_songs_data
                         ''')
        
        # Sort the DataFrame by CALLSIGN and TIME
        windowSpec = Window.partitionBy("CALLSIGN").orderBy("TIME")

        # Calculate the duration difference between consecutive rows within the same CALLSIGN group
        data = data.withColumn("next_time", lag("TIME").over(windowSpec))
        data = data.withColumn("duration_diff", when(col("next_time").isNotNull(), datediff(col("next_time"), col("TIME"))).otherwise(0))

        # Count the number of songs with the same start time and call sign
        windowSpec = Window.partitionBy("CALLSIGN", "TIME")
        data = data.withColumn("songs_count", count("UNIQUE_ID").over(windowSpec))

        # Divide the duration difference by the number of songs with the same start time and call sign
        data = data.withColumn("average_duration", col("duration_diff") / col("songs_count"))

        # Drop unnecessary columns
        data = data.drop("next_time", "duration_diff", "songs_count")

        # write data as table
        data.write.mode("overwrite").saveAsTable(f"{db}.songs_with_duration")
