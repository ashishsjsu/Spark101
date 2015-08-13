#import
import sys
from pyspark import SparkContext, SparkConf


APP_NAME = "Explore Drawbridge Data"


def configureSpark():
	#Configure SPARK
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	return sc


def getDeviceRDD(sc, inputfile):
	devices = sc.textFile(inputfile)
	return devices

def getCookieRDD(sc, inputfile):
	cookies = sc.textFile(inputfile)
	return cookies

def xploreDevices(sc, inputfile):

	devices = sc.textFile(inputfile)
	device_fields = devices.map(lambda line: line.split(","))
	device_fields.persist()
	return device_fields

	


