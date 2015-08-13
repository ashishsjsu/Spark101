#imports

import sys
from pyspark import SparkContext, SparkConf, StorageLevel


def configureSpark(app_name, master):
	
	#Configure SPARK
	conf = SparkConf().setAppName(app_name)
	conf = conf.setMaster(master)
	spark_context = SparkContext(conf=conf)
	return spark_context


def getDeviceFields(sc, inputFile):
	
	#1)Load devices.csv
	df = sc.textFile(inputFile)
	device_fields = df.map(lambda line: line.split(","))

	# persist the field as it is needed again
	device_fields.persist()
	
	# print num of partitions of RDD
	print "Number of partitions for device_fields RDD : %d" % device_fields.getNumPartitions()		
	
	return device_fields	


def getCookieFields(sc, inputFile):
	
	#2)Load cookies.csv 
	cookies = sc.textFile(inputFile)
	cookie_fields = cookies.map(lambda line: line.split(','))
	# persist the field as it is needed again
	cookie_fields.persist()
	return cookie_fields


def getPropertyCategoryFields(sc, inputFile):
	
	#3) Load property_categories.csv
	property_categories = sc.textFile(inputFile);
	property_fields = property_categories.map(lambda x: x.split(','))
	# persist the field as it is needed again 
	property_fields.persist()
	return property_fields


def getIDIPFields(sc,  inputFile):
	
	id_ipsRDD = sc.textFile(inputFile)
 	id_ips_fields = id_ipsRDD.map(lambda field: field.split(','))
	# persist RDD in disk	
	id_ips_fields.persist(StorageLevel.DISK_ONLY)
	
	# print num of partitions of RDD
	print "Number of partitions for id_ips_fields RDD : %d" % id_ips_fields.getNumPartitions()		
	
	return id_ips_fields


def deviceStatistics(device_fields):

	total_records = device_fields.map(lambda fields: fields[0]).count()

	num_uniq_handles = device_fields.filter(lambda x: "-1" not in x[0]).count() 

	num_unique_deviceid = device_fields.map(lambda fields: fields[1]).distinct().count()
	
	num_unique_devicetypes = device_fields.map(lambda fields: fields[2]).distinct().count()

	num_unique_deviceos = device_fields.map(lambda fields: fields[3]).distinct().count()

	num_unique_dev_country = device_fields.map(lambda fields: fields[4]).distinct().count()

	num_missing_handles = device_fields.filter(lambda h: "-1" in h[0]).count()

	percentage_missing_handles = ( int(num_missing_handles) / int(total_records) ) * 100


	 
	print "Statistics from devices.csv: \n  total_records :%d, \n  num_uniq_handles :%d, \n num_unique_deviceid :%d, \n num_unique_devicetypes :%d, \n num_unique_deviceos :%d, \n num_unique_dev_country: %d, \n num_missing_handles: %d  \n percentage_missing_handles: %d \n " % (total_records, num_uniq_handles, num_unique_deviceid, num_unique_devicetypes, num_unique_deviceos, num_unique_dev_country, num_missing_handles, percentage_missing_handles )


def cookieStatistics(cookie_fields):

	
	tot_cookie_records = cookie_fields.map(lambda f: f[0]).count()

	num_uniq_handles_cookie = cookie_fields.filter(lambda x: "-1" not in x[0]).count()

	num_uniq_cookie_id = cookie_fields.map(lambda f: f[1]).count()

	num_uniq_comp_os = cookie_fields.map(lambda f: f[2]).distinct().count()

	num_uniq_browser_versions = cookie_fields.map(lambda f: f[3]).distinct().count()

	num_uniq_cookie_country = cookie_fields.map(lambda f: f[4]).distinct().count()

	missing_handles_cookies = cookie_fields.filter(lambda c: "-1" in c[0]).count()

	percentage_missing_handles = ( int(missing_handles_cookies) / int(tot_cookie_records) ) * 100


	print "\n"

	print "Statistics from cookies.csv : \n tot_cookie_records :%d, \n num_uniq_handles_cookie :%d, \n num_uniq_cookie_id :%d, \n  num_unique_comp_os :%d, \n  num_uniq_browser_versions :%d, \n  num_uniq_cookie_country: %d  num_missing_handles_cookies: %d \n percentage_missing_handles: %d  \n" % (tot_cookie_records, num_uniq_handles_cookie, num_uniq_cookie_id, num_uniq_comp_os, num_uniq_browser_versions, num_uniq_cookie_country,  missing_handles_cookies , percentage_missing_handles)




def propertyCategoriesStatistics(property_fields):

	num_uniq_property_fields = property_fields.map(lambda f: f[0]).distinct().count()

	num_uniq_property_categories = property_fields.map(lambda f: f[1]).distinct().count()

	print "\n"

	print "Statistics from property_categories.csv: \n num_uniq_property_fields: %d,  \n num_uniq_property_categories: %d \n" % (num_uniq_property_fields, num_uniq_property_categories) 



def idIPStatistics(id_ip_fields):
	
	tot_records = id_ip_fields.map(lambda fields: fields[0]).count()
	
	num_unique_devices = id_ip_fields.filter(lambda x: '0' in x[1]).distinct().count()

	num_unique_cookies = id_ip_fields.filter(lambda x: '1' in x[1]).distinct().count()
	
	distinct_ip_count = id_ip_fields.map(lambda x:  x[2]).distinct().count()

	ip_freq_RDD = id_ip_fields.map(lambda x:  x[3])
 
	maxi =  ip_freq_RDD .max()
	mini = ip_freq_RDD .min()
#	max_ip_freq = id_ip_fields.map(lambda x:  x[3]).max()
	
#	min_ip_freq = id_ip_fields.map(lambda x:  x[3]).min()

	print "Statitistics from id_ips.csv: \n tot_records: %d,  \n num_unique_devices: %d, \n num_unique_cookies: %d, \n distinct_ips_count: %d, \n  max_ip_freq: %d, \n min_freq_count: %d \n ", (tot_records, num_unique_devices, num_unique_cookies, distinct_ips_count,  max_ip_freq, min_freq_count)




def getStatistics(dataDir, deviceFile, cookieFile, propertyFile, idIPFile):
	
	#local[#] - # is nnumber of cores to be allocated (Max =max cores on your system)
	sparkContext = configureSpark("StatisticAnalysis DrawbridgeData", "local[*]")
	
	# store original  sys.stdout to restore it later 
	sys_stdout = sys.stdout
	#redirect output to file
	sys.stdout = open("/home/ken/Desktop/out1.text", "w")

	
	deviceFields = getDeviceFields(sparkContext, dataDir+"/"+deviceFile)
	deviceStatistics(deviceFields)	

	#cookieFields = getCookieFields(sparkContext, dataDir+"/"+cookieFile)
	#cookieStatistics(cookieFields)

	#propertyFields = getPropertyCategoryFields(sparkContext, dataDir+"/"+propertyFile)
	#propertyCategoriesStatistics(propertyFields)	
	
	id_ipFields = getIDIPFields(sparkContext, dataDir+"/"+ idIPFile )
	idIPStatistics(id_ipFields)
	
	
	#restore original stdout
	sys.stdout = sys_stdout


getStatistics('/home/ken/Downloads/ASHISH_Data/DrawbridgeData', "devices.csv", "cookies.csv", "property_categories.csv", "id_ips.csv")


