import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_161/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import unix_timestamp, from_unixtime

# Creating spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("datewise_booking") \
    .getOrCreate()	
sc = spark.sparkContext
sc

# Reading the data 
df1 = spark.read.csv("/user/root/bookings_data/part-m-00000",inferSchema = True)

# Renaming the column names
df1 = df1.withColumnRenamed("_c0","booking_id")\
	   .withColumnRenamed("_c1","customer_id") \
	   .withColumnRenamed("_c2","driver_id") \
	   .withColumnRenamed("_c3","customer_app_version")  \
	   .withColumnRenamed("_c4","customer_phone_os_version") \
	   .withColumnRenamed("_c5","pickup_lat") \
	   .withColumnRenamed("_c6","pickup_lon") \
	   .withColumnRenamed("_c7","drop_lat") \
	   .withColumnRenamed("_c8","drop_lon") \
	   .withColumnRenamed("_c9","pickup_timestamp")  \
	   .withColumnRenamed("_c10","drop_timestamp")  \
	   .withColumnRenamed("_c11","trip_fare") \
	   .withColumnRenamed("_c12","tip_amount")  \
	   .withColumnRenamed("_c13","currency_code") \
	   .withColumnRenamed("_c14","cab_color")  \
	   .withColumnRenamed("_c15","cab_registration_no") \
	   .withColumnRenamed("_c16","customer_rating_by_driver")  \
	   .withColumnRenamed("_c17","rating_by_customer")  \
	   .withColumnRenamed("_c18","passenger_count")

# creating a new column named date and taking the data from pickup_timestamp after formatting the date	   
df1 = df1.withColumn("date", date_format('pickup_timestamp', "yyyy-MM-dd"))

# Displaying the top 5 results 
df1.show(5)

# Grouping the data datewise and getting the count & storing it in date dataframe 
date = df1.select('date').groupBy('date').count()

date.show() # displaying the data 
date.count() # displaying the count 

# writing the data back to HDFS & saving it 
date.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('datewise_aggregration', header = 'true')
df1.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('bookings_data_csv', header = 'true')
