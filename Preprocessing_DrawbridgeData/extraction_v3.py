import explore_v2 as ex
import sys
from pyspark import StorageLevel
import pickle

#pickle is a python module that serializes python object structure into byte stream and deserializes byte stream back to python objct structure 

PATH = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/'
COOKIE_FILE_RAW = PATH + 'cookies.csv'
DEVICE_FILE_RAW = PATH + 'device_cookie.csv'
DATA_FILE = PATH + 'complete_train200K.csv'
TEST_FILE = PATH + 'test_all1.csv'

TEST_DIR = PATH + 'Extraction/test_all1'
OUTPUT_DIR = PATH + 'Extraction/complete_train2'

#cookie dictionaries
COMP_OS_TYPES_DICT = {}
BROWSER_VERSION_DICT = {}
COOKIE_COUNTRY_DICT = {}
#device dictionaries
DEVICE_TYPES_DICT = {}
DEVICE_OS_DICT = {} 

#categorical features we want to map
COMP_OS_TYPES = ""
BROWSER_VERSION = ""
COOKIE_COUNTRY = ""


DEVICE_TYPES = ""
DEVICE_OS = ""


#map device's and cookie's categorical features in joined file to numeric 
def device_cookieMapper(line):	
	
	fields = line.split(',')
	dev_type = fields[3]
	#replae categorical feature with corresponding numeric value from dictionary
	s1 = line.replace(dev_type, DEVICE_TYPES_DICT[str(dev_type)])
	
	dev_os = fields[4]
	s2 = s1.replace(dev_os, DEVICE_OS_DICT[str(dev_os)])
	
	dev_country = fields[7]
	s3 = s2.replace(dev_country, COOKIE_COUNTRY_DICT[str(dev_country)])
	
	comp_os_type = fields[5]
	s4 = s3.replace(comp_os_type, COMP_OS_TYPES_DICT[str(comp_os_type)])
	
	browser_version = fields[6]
	s5 = s4.replace(browser_version, BROWSER_VERSION_DICT[str(browser_version)])

	s6 = s5.replace("id_", '');

	return s6



def mapTestCategoricalFeatures():
	
	#get spark context from import specified
	spark_contxt = ex.configureSpark()
	testDataRDD = ex.getDeviceRDD(spark_contxt, TEST_FILE)
	s = testDataRDD.map(testMapper)
	s.saveAsTextFile(TEST_DIR)	

def testMapper(line):
	
	fields = line.split(',')
	dev_type = fields[1]
	
	#open the pickle file for device types dictionary
	with open(PATH + 'dictionary/dev-type-dict.pickle', 'rb') as f:
		#deserialize device type dictionary pickle file
		dev_type_dict = pickle.load(f)
		s1 = line.replace(dev_type, dev_type_dict[str(dev_type)])

	device_os_type = fields[2]		

	#open the pickle file for device os dictionary	
	with open(PATH + 'dictionary/device-os-dict.pickle', 'rb') as f:
		device_os_dict = pickle.load(f)
		s2 = s1.replace(device_os_type, device_os_dict[str(device_os_type)])	

	comp_os_type = fields[4]	
	
	#open the pickle file
	with open(PATH + 'dictionary/comp-os-type-dict.pickle', 'rb') as f:
		comp_os_type_dict = pickle.load(f)
		s3 = s2.replace(comp_os_type, comp_os_type_dict[str(comp_os_type)])
	
	browser_version = fields[5]
	with open(PATH + 'dictionary/browser-version-dict.pickle', 'rb') as f:
		browser_version_dict =	pickle.load(f)
		s4 = s3.replace(browser_version, browser_version_dict[str(browser_version)])


	country = fields[6]
	with open(PATH + 'dictionary/cookie-country-dict.pickle', 'rb') as f:
		country_dict = pickle.load(f)
		s5 = s4.replace(country, country_dict[str(country)])
	
	s6 = s5.replace('id_', '')

	return s6

def mapCategoricalFeatures():

	#get spark context from import specified
	spark_contxt = ex.configureSpark()
	#get cookie RDD
	cookieRDD = ex.getCookieRDD(spark_contxt, COOKIE_FILE_RAW)
	#retrieve cookie fields	
	cookie_fields = cookieRDD.map(lambda line: line.split(","))
	#persist the cookie fields	
	cookie_fields.persist(StorageLevel.DISK_ONLY)	

	global COMP_OS_TYPES
	COMP_OS_TYPES = cookie_fields.map(lambda field: field[2]).distinct().collect()
	index = 0
	for o in COMP_OS_TYPES:
		COMP_OS_TYPES_DICT[str(o)] = str(index)
		index = index + 1
	
	# serialize COMP_OS_TYPES_DICT with pickle(store it in serialized format, can be deserialized later to use)
	with open(PATH + "dictionary/comp-os-type-dict.pickle",'wb') as f:
    		pickle.dump(COMP_OS_TYPES_DICT, f)
	
		
	global BROWSER_VERSION
	BROWSER_VERSION = cookie_fields.map(lambda field: field[3]).distinct().collect()
	index = 0
	for b in BROWSER_VERSION:
		BROWSER_VERSION_DICT[str(b)] = str(index)
		index = index + 1

	#serialize BROWSER_VERSION_DICT with pickle (store it in serialized format, can be deserialized later to use)
	with open(PATH + "dictionary/browser-version-dict.pickle", 'wb') as f:
		pickle.dump(BROWSER_VERSION_DICT, f)
	
	
	global COOKIE_COUNTRY
	COOKIE_COUNTRY = cookie_fields.map(lambda field: field[4]).distinct().collect()
	index = 0
	for c in COOKIE_COUNTRY:
		COOKIE_COUNTRY_DICT[str(c) ]= str(index)
		index = index + 1 	
	
	# serialize python dictionary object into a file with pickle
	with open(PATH + "dictionary/cookie-country-dict.pickle", "wb") as f:
		pickle.dump(COOKIE_COUNTRY_DICT, f)
	

	deviceRDD = ex.getDeviceRDD(spark_contxt, DEVICE_FILE_RAW)
	device_fields = deviceRDD.map(lambda line: line.split(','))
	device_fields.persist(StorageLevel.DISK_ONLY)
	
	#get device types
	global DEVICE_TYPES
	DEVICE_TYPES = device_fields.map(lambda field: field[3]).distinct().collect()
	#create device type feature map to numeric values andd store in dictionary 
	index = 0
    	for d in DEVICE_TYPES:
		DEVICE_TYPES_DICT[str(d)] = str(index)
		index = index + 1	
	
	with open(PATH + "dictionary/dev-type-dict.pickle", 'wb') as f:
		pickle.dump(DEVICE_TYPES_DICT, f)
	
	#get device os
	global DEVICE_OS	
	DEVICE_OS = device_fields.map(lambda field: field[4]).distinct().collect()
	#create device os feature map to numeric values andd store in dictionary
	index = 0
	for o in DEVICE_OS:
		DEVICE_OS_DICT[str(o)] = str(index)
		index = index + 1		

	with open(PATH + "dictionary/device-os-dict.pickle", 'wb') as f:
		pickle.dump(DEVICE_OS_DICT, f) 
	
	saveExtractedData(spark_contxt)


def saveExtractedData(spark_contxt):
	
	dataRDD = ex.getDeviceRDD(spark_contxt, DATA_FILE)		

	#replace device type in each line and write to new file
	s = dataRDD.map(device_cookieMapper)
	#save transformed daata to file 
	s.saveAsTextFile(OUTPUT_DIR)
	#s.saveAsTextFile(TEST_DIR)



#mapCategoricalFeatures()

mapTestCategoricalFeatures()

'''

with open(PATH + "cookie-country-dict.pickle", "rb") as f:
	c = pickle.load(f)
	print c


dictionary = open('/home/ashish/Desktop/dictionary.txt', 'w')
sys.stdout = dictionary

# deserialize COMP_OS_TYPES dictionary object using pickle
with open("comp-os-type-dict.pickle",'rb') as f:
	a = pickle.load(f)
	print a	

with open("browser-version-dict.pickle", 'rb') as f:
	b = pickle.load(f)
	#print b

with open("cookie-country-dict.pickle", "rb") as f:
	c = pickle.load(f)
	#print c['country_146']

with open('dev-type-dict.pickle', 'rb') as f:
	d = pickle.load(f)
	print d

with open('device-os-dict.pickle', 'rb') as f:
	e = pickle.load(f)
	print e
'''
