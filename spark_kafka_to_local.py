from pyspark.sql import SparkSession

# Creating a spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Kafka To HDFS") \
    .getOrCreate()
	
# Here we're creating a dataframe from kafka data that we've received 
df1 = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
    .option("subscribe","de-capstone3") \
    .option("startingOffsets","earliest") \
    .load()
df1.printSchema()

# Here we're Transfroming  dataframe by dropping few columns and changing value column data type
df1 = df1.withColumn('value_str', df1['value'].cast('string').alias('key_str')).drop('value') \
        .drop('key','topic','partition','offset','timestamp','timestampType')
		
# Here we're writing the dataframe to local file directory and running it until it gets terminated 
df1.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("truncate","false") \
  .option("path", "clickstream_data") \
  .option("checkpointLocation","clickstream_checkpoint") \
  .start() \
  .awaitTermination()
