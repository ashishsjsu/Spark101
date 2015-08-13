import sys
from pyspark import StorageLevel
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF
import pickle

PATH = '/home/ashish/Downloads/click-fraud-data/'

FILEPATH = '/home/ashish/Downloads/click-fraud-data/clicks_09feb12.csv'

APP_NAME = "Extraction"

fieldType1 = ""
FIELD1_DICT = {}

def mapCategoricalFeatures():

	#get spark context
	spark_contxt = configureSpark()
	#get RDD
	dataRDD = getDataRDD(spark_contxt, FILEPATH)
	#retrieve fields	
	_fields = dataRDD.map(lambda line: line.split(","))
	#persist the cookie fields	
	_fields.persist(StorageLevel.DISK_ONLY)	

	global fieldType1
	fieldType1 = _fields.map(lambda field: field[2]).distinct().collect()
	index = 0
	for o in fieldType1:
		FIELD1_DICT[str(o)] = str(index)
		index = index + 1
	
	# serialize COMP_OS_TYPES_DICT with pickle(store it in serialized format, can be deserialized later to use)
	with open(PATH + "dictionary/field1.pickle",'wb') as f:
    		pickle.dump(FIELD1_DICT, f)

	print FIELD1_DICT

def configureSpark():
	#Configure SPARK
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	return sc


def getDataRDD(sc, inputfile):
	devices = sc.textFile(inputfile)
	return devices


mapCategoricalFeatures()
