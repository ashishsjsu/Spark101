import explore_v2 as ex
import sys
from pyspark import StorageLevel
import pickle

#pickle is a python module that serializes python object structure into byte stream and deserializes byte stream back to python objct structure 

COOKIE_FILE_RAW = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv'

#cookie dictionaries
COMP_OS_TYPES_DICT = {}
BROWSER_VERSION_DICT = {}
COOKIE_COUNTRY_DICT = {}

#distinct comp os type RDD
COMP_OS_TYPES = ""
BROWSER_VERSION = ""
COOKIE_COUNTRY = ""


def mapCookieCategoricalFeatures():

	#get spark context from import specified
	spark_contxt = ex.configureSpark()
	#get device RDD
	dataRDD = ex.getCookieRDD(spark_contxt, COOKIE_FILE_RAW)
	#retrieve device fields	
	data_fields = dataRDD.map(lambda line: line.split(","))
	dataRDD.persist(StorageLevel.DISK_ONLY)	

	global COMP_OS_TYPES
	COMP_OS_TYPES = data_fields.map(lambda field: field[2]).distinct().collect()
	index = 0
	for o in COMP_OS_TYPES:
		COMP_OS_TYPES_DICT[str(o)] = str(index)
		index = index + 1
	
	# serialize COMP_OS_TYPES_DICT with pickle
	with open("comp-os-type-dict.pickle",'wb') as f:
    		pickle.dump(COMP_OS_TYPES_DICT, f)
	
		
	global BROWSER_VERSION
	BROWSER_VERSION = data_fields.map(lambda field: field[3]).distinct().collect()
	index = 0
	for b in BROWSER_VERSION:
		BROWSER_VERSION_DICT[str(b)] = str(index)
		index = index + 1

	#serialize BROWSER_VERSION_DICT with pickle
	with open("browser-version-dict.pickle", 'wb') as f:
		pickle.dump(BROWSER_VERSION_DICT, f)
	
	
	global COOKIE_COUNTRY
	COOKIE_COUNTRY = data_fields.map(lambda field: field[4]).distinct().collect()
	index = 0
	for c in COOKIE_COUNTRY:
		COOKIE_COUNTRY_DICT[str(c) ]= str(index)
		index = index + 1 	
	
	# serialize python dictionary object with pickle
	with open("cookie-country-dict.pickle", "wb") as f:
		pickle.dump(COOKIE_COUNTRY_DICT, f)
	
	
mapCookieCategoricalFeatures()

# deserialize COMP_OS_TYPES dictionary object using pickle
with open("comp-os-type-dict.pickle",'rb') as f:
	a = pickle.load(f)
	#print a	

with open("browser-version-dict.pickle", 'rb') as f:
	b = pickle.load(f)
	#print b

print "*********************************************************"
with open("cookie-country-dict.pickle", "rb") as f:
	c = pickle.load(f)
	print c['country_146']
