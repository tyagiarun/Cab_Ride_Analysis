from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Creating spark session
spark = SparkSession.builder \
    .master("local") \
    .appName("Kafka To HDFS") \
    .getOrCreate()

# Here we're Reading json data (real-time data) into dataframe 
df1 = spark.read.json('clickstream_data/part-00000-62b5e35a-b98c-4d44-ae6a-5774b3549b0e-c000.json')

# extrating columns from jason value in dataframe and create new dataframe with new cloumns 
df1 = df1.select(\
	get_json_object(df1["value_str"],"$.customer_id").alias("customer_id"),\
	get_json_object(df1["value_str"],"$.app_version").alias("app_version"),\
	get_json_object(df1["value_str"],"$.OS_version").alias("OS_version"),\
	get_json_object(df1["value_str"],"$.lat").alias("lat"),\
	get_json_object(df1["value_str"],"$.lon").alias("lon"),\
	get_json_object(df1["value_str"],"$.page_id").alias("page_id"),\
	get_json_object(df1["value_str"],"$.button_id").alias("button_id"),\
	get_json_object(df1["value_str"],"$.is_button_click").alias("is_button_click"),\
	get_json_object(df1["value_str"],"$.is_page_view").alias("is_page_view"),\
	get_json_object(df1["value_str"],"$.is_scroll_up").alias("is_scroll_up"),\
	get_json_object(df1["value_str"],"$.is_scroll_down").alias("is_scroll_down"),\
	get_json_object(df1["value_str"],"$.timestamp").alias("timestamp"),\
	)

# Here we are printing schema of dataframe with new columns
print(df1.schema)	

#print 10 records from dataframe
df1.show(10)

# Save dataframe to csv file with headers in first row in local file directory
df1.coalesce(1).write.format('com.databricks.spark.csv').mode('overwrite').save('user/root/clickstream_data_flatten', header = 'true')
