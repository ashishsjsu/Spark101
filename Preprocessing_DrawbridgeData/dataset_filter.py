import explore.py as explore
import sys
from pyspark import SparkContext, SparkConf

DEVICES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices.csv'
COOKIES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv'
DEVICES_CLEAN_DIR = "/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices_clean"
COOKIES_CLEAN_DIR  = "/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies_clean"

def filterDevices(sc):

	#1)Load devices.csv
	deviceRDD = sc.textFile(DEVICES_FILE)
	device_fields = deviceRDD.map(lambda line: line.split(","))
	# persist the field as it is needed again
	device_fields.persist()

	# Filtering missing data - remove entries with drawbridge_handle = '-1'
	devices_train = device_fields.filter(lambda field: "-1" not in field[0])
	
	file = DEVICES_CLEAN_DIR
	devices_train.saveAsTextFile(file);
	

def filterCookies(sc):
	
	cookiesRDD = sc.textFile(COOKIES_FILE)	
	cookie_fields = cookiesRDD.map(lambda line: line.split(','))
	# persist the field as it is needed again
	cookie_fields.persist()

	# Filtering missing data - remove entries with drawbridge_handle = '-1'
	cookies_train = cookie_fields.filter(lambda field: "-1" not in field[0])
	file = COOKIES_CLEN_DIR
	cookies_train.saveAsTextFile(file)


spark_context = explore.configureSpark('Filter Job', 'local')

filterDevices(spark_context)
filterCookies(spark_context)






