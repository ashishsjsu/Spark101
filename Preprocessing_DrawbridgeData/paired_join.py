import explore_v2 as explore

DEVICE_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices.csv'
COOKIES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv'
OUT_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices_cookies.csv'


def joinRDD():
	
	spark_context = explore.configureSpark()	
	#devices RDD
	devices = explore.getDeviceRDD(spark_context, DEVICES_FILE)
	#cookie RDD	
	cookies = explore.getDeviceRDD(spark_context, COOKIES_FILE)
		
	#create key value pairs (drawbridge_handle, devices_data)
	device_pairs = devices.map(lambda line: (line.split(",")[0], line))
	##create key value pairs (drawbridge_handle, cookies_data)
	cookie_pairs = cookies.map(lambda linec: (linec.split(",")[0], linec))

	#join devices and cookies key, val pairs on common key
	join_val = device_pairs.join(cookie_pairs)	

	join_val.saveAsTextFile()

joinRDD()

