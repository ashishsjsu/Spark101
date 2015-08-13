import explore_v2 as ex
import numpy as np


DEVICES_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/devices.csv'


DATA_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/device_cookie.csv'
OUTPUT_DIR = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/Extraction/device_cookie2'

NEG_DATA_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/neg_200.csv'
NEG_OUTPUT_DIR = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/Extraction/device_cookie_neg'


#device dictionaries

DEVICE_TYPES_DICT = {}
DEVICE_OS_DICT = {} 
DEVICE_COUNTRY_DICT = {}
ANON_C1_DICT = {}
ANON_C2_DICT = {}

#distinct device types RDD
DEVICE_TYPES = ""
#distinct device OS RDD
DEVICE_OS = ""
#distinct device country RDD
DEVICE_COUNTRY = ""  
ANON_C1 = ""
ANON_C2 = ""


#cookie dictionaries
COMP_OS_TYPES_DICT = {}
BROWSER_VERSION_DICT = {}

#distinct comp os type RDD
COMP_OS_TYPES = ""
BROWSER_VERSION = ""


#get device types RDD frm devices.csv
def extractDeviceTypes():
	
	spark_contxt = ex.configureSpark()
	
	#retrieve device fields RDD
	device_fields = ex.xploreDevices(spark_contxt, DEVICES_FILE)
	
	#collect works only if data is small enough to fit in memory of driver machine
	#otherwise store RDD using saveAsTextFile to hdfs cluster

	device_types = device_fields.map(lambda field: field[2]).distinct().collect()
	device_types.sort()
	return device_types


	

#map device's categorical features to numeric 
def device_cookieMapper(line):	
	
	fields = line.split(',')
	dev_type = fields[3]
	s1 = line.replace(dev_type, DEVICE_TYPES_DICT[str(dev_type)])
	
	dev_os = fields[4]
	s2 = s1.replace(dev_os, DEVICE_OS_DICT[str(dev_os)])
	
	dev_country = fields[7]
	s3 = s2.replace(dev_country, DEVICE_COUNTRY_DICT[str(dev_country)])
	
	comp_os_type = fields[5]
	s4 = s3.replace(comp_os_type, COMP_OS_TYPES_DICT[str(comp_os_type)])
	
	browser_version = fields[6]
	s5 = s4.replace(browser_version, BROWSER_VERSION_DICT[str(browser_version)])

	s6 = s5.replace("id_", '');

	'''
	dev_anonc1 = v[6]
	s4 = s3.replace(dev_anonc1, ANON_C1_DICT[str(dev_anonc1)]) 	

	dev_anonc2 = v[7]
	s5 = s4.replace(dev_anonc2, ANON_C2_DICT[str(dev_anonc2)])
	'''
	return s6


#create dictionary for each categorical feature
def mapCategoricalFeatures():
	
	#get spark context from import specified
	spark_contxt = ex.configureSpark()
	#get device RDD
	dataRDD = ex.getDeviceRDD(spark_contxt, DATA_FILE)
	#retrieve device fields	
	data_fields = dataRDD.map(lambda line: line.split(","))
	
	dataRDD.persist()	

	#get device types
	global DEVICE_TYPES
	DEVICE_TYPES = data_fields.map(lambda field: field[3]).distinct().collect()
	#create device type feature map to numeric values andd store in dictionary 
	index = 0
    	for d in DEVICE_TYPES:
		DEVICE_TYPES_DICT[str(d)] = str(index)
		index = index + 1	
	
	#get device os
	global DEVICE_OS	
	DEVICE_OS = data_fields.map(lambda field: field[4]).distinct().collect()
	#create device os feature map to numeric values andd store in dictionary
	index = 0
	for o in DEVICE_OS:
		DEVICE_OS_DICT[str(o)] = str(index)
		index = index + 1		
	
	#get device country
	global DEVICE_COUNTRY	
	DEVICE_COUNTRY = data_fields.map(lambda field: field[7]).distinct().collect()
	#create device country feature map to numeric values andd store in dictionary
	index = 0
	for c in DEVICE_COUNTRY:
		DEVICE_COUNTRY_DICT[str(c)] = str(index)
		index = index + 1


	#get comp os type
	global COMP_OS_TYPES
	COMP_OS = data_fields.map(lambda field: field[5]).distinct()
	COMP_OS_TYPES_COUNT = COMP_OS.count() 
	COMP_OS_TYPES = COMP_OS.collect()	
	print "Distinct COMP OS ", COMP_OS_TYPES_COUNT
	index = 0
	for o in COMP_OS_TYPES:
		COMP_OS_TYPES_DICT[str(o)] = str(index)
		index = index + 1
	print "COMP OS dctionary size ", len(COMP_OS_TYPES_DICT)	
	

	#get browser version
	global BROWSER_VERSION
	BROWSER_VERSION = data_fields.map(lambda field: field[6]).distinct().collect()
	index = 0
	for b in BROWSER_VERSION:
		BROWSER_VERSION_DICT[str(b)] = str(index)
		index = index + 1

	
	'''
	#get anonymous_c1 feature
	global ANON_C1
	ANON_C1 = data_fields.map(lambda field: field[6]).distinct().collect()
	#create categorical-to-numeric mapping in dictionary 
	index = 0
	for a1 in ANON_C1:
		ANON_C1_DICT[str(a1)] = str(index)
		index = index + 1 	


	#get anonymous_c2 feature
	global ANON_C2
	ANON_C2 = data_fields.map(lambda field: field[7]).distinct().collect()
	#create categorical-to-numeric mapping in dictionary 
	index = 0
	for a2 in ANON_C2:
		ANON_C2_DICT[str(a2)] = str(index)
		index = index + 1	
	'''

	
	print DEVICE_TYPES_DICT
	print DEVICE_OS_DICT 
	print DEVICE_COUNTRY_DICT
	print COMP_OS_TYPES_DICT
	print BROWSER_VERSION_DICT


	#replace device type in each line and write to new file
	#s = dataRDD.map(device_cookieMapper)

	#save transformed daata to file 
	#s.saveAsTextFile(NEG_OUTPUT_DIR)



#get binary feature vector for categorical data (works for device types)
def getBinaryFeatureVector(device_types):
	
	index = 0
	#create a dictionary to store device type against index	
	device_types_dict = {}
	
    	for d in device_types:
		print d	
		device_types_dict[d] = index
		index = index + 1
	
	print "devtype_2 is encoded to : %d"  % device_types_dict['devtype_2']
	
	L = len(device_types_dict) 
	#bin_x = np.zeros(L)
	
	print "Binary feature vectors:"
	for d in device_types:
	    	bin_x = np.zeros(L)
		dev_2_idx = device_types_dict[d]
    		bin_x[dev_2_idx] = 1
    		print "%s: %s" % (d, bin_x)


#dev_types = extractDeviceTypes()
#getBinaryFeatureVector(dev_types)

mapCategoricalFeatures()

#mapCookieCategoricalFeatures()
