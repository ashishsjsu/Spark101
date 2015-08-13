#Exploring data with Dataframes

#Command : bin/spark-submit --packages com.databricks:spark-csv_2.10:1.1.0  /home/ken/Downloads/ASHISH_Data/DrawbridgeData/Scripts/explore_df.py

import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

DEVICES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices.csv'
COOKIES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv'


def configureSpark():
	#Configure SPARK
	conf = SparkConf().setAppName("a")
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	return sc

scon = configureSpark()
sqlContext = SQLContext(scon)

#create dataframe from CSV. Use spark-csv package
_devices = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(DEVICES_FILE)

#Show contents
#_devices.show()

#Show Schema
_devices.printSchema()

#save as table
devices = _devices.registerAsTable("devices")

#get data using Select sql query
devices_data = sqlContext.sql("Select * from devices limit 100").collect()

#print devices_data

_cookies = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(COOKIES_FILE)

_cookies.printSchema()

cookies = _cookies.registerAsTable("cookies")

cookies_data = sqlContext.sql("Select * from cookies limit 100").collect()

#print cookies_data


#try to join the devices and cookies


devices_cookies = sqlContext.sql("Select * from devices join cookies on devices.drawbridge_handle_devices = cookies.drawbridge_handle")

table_devices_cookies = devices_cookies.registerAsTable('devices_cookies')

#devices_cookies.printSchema()

devices_cookies_samples = sqlContext.sql("Select * from devices_cookies limit 100")

final = devices_cookies_samples.rdd

final.saveAsTextFile('/home/ken/Desktop/demo.csv')

#print devices_cookies_samples.take(1)

#devices_cookies_samples.write.save('/home/ken/Desktop/temp.parquet', format='parquet')
#out = sqlContext.read.parquet('/home/ken/Desktop/temp.parquet')
#out.registerAsTable('parquetOut')
#sample = sqlContext.sql("Select * from parquetOut limit 1").collect()
#fileout = open('/home/ken/Desktop/dummy.csv', "w")
#sys.stdout = fileout
#print sample
