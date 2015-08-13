import sys
from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark import StorageLevel
import tempfile


HADOOPDIR = 'hdfs://localhost:54310/user/'
CLICKS_HDPFILEPATH = HADOOPDIR + 'data/click_fraud/clicks_23feb12.csv'
PUBLISHERS_HDPFILEPATH = HADOOPDIR + 'data/click_fraud/publishers_23feb12.csv'

PATH = '/home/ashish/Downloads/click-fraud-data/'
CLICKS_FILEPATH = PATH + 'clicks_09feb12.csv'
PUBLISHERS_FILEPATH = PATH + 'publishers_09feb12.csv'


def configureSpark():
	#Configure SPARK
	conf = SparkConf().setAppName("a")
	conf = conf.setMaster("local[*]")
	conf = conf.set("spark.executor.memory", "2g").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").set("spark.kryoserializer.buffer", "256").set("spark.akka.frameSize", "500").set("spark.rpc.askTimeout", "30").set('spark.executor.cores', '4').set('spark.driver.memory','2g')

	sc = SparkContext(conf=conf)
	return sc



def getDataFrame(path):
	
	scon = configureSpark()
	sqlContext = SQLContext(scon)
	#read csv file using sqlcontext and create a dataframe
	df = sqlContext.read.format('com.databricks.spark.csv').options(headers='true').load(path)
	return df



def mapClickCategoricalFeatures():
		

	indexed = ""

	df = getDataFrame(CLICKS_HDPFILEPATH)
	
	df.persist(StorageLevel.DISK_ONLY)

	print df.columns
	
	#select columns to be mapped
	click_cols = ["C2", "C3", "C4", "C5", "C7", "C8"]

	for col in click_cols:

		if(indexed == ""):	
			indexed = df
	
		print indexed
		outcol = col+"Index"
		indexer = StringIndexer(inputCol=col, outputCol=outcol)
		indexed = indexer.fit(indexed).transform(indexed)

	indexed.show()

	indexed.persist(StorageLevel.DISK_ONLY)

	#indexed.select('C0', 'C1', 'C2Index', 'C3Index', 'C4Index', 'C5Index', 'C6', 'C7Index', 'C8Index').write.format('com.databricks.spark.csv').save(PATH+"extraction/clicks1.csv")


	indexed.select('C0', 'C1', 'C2Index', 'C3Index', 'C4Index', 'C5Index', 'C6', 'C7Index', 'C8Index').write.format('com.databricks.spark.csv').save(HADOOPDIR+"data/click_fraud/extraction/clicks_23feb12.csv")




def mapPublisherCategoricalFeatures():
	
	indexed = ""

	df = getDataFrame(PUBLISHERS_HDPFILEPATH)

	df.persist(StorageLevel.DISK_ONLY)

	print df.columns
	
	publisher_cols = ["C0", "C1", "C2", "C3"]
	
	for col in publisher_cols:

		if(indexed == ""):	
			indexed = df

		print indexed
		outcol = col+"Index"
		#stringindexer maps each value in inout colun into a double indexed value and creates a new column in dataframe
		indexer = StringIndexer(inputCol=col, outputCol=outcol)
		#fit and transform the columns using indexer		
		indexed = indexer.fit(indexed).transform(indexed)

	indexed.show()

	indexed.persist(StorageLevel.DISK_ONLY)

	indexed.select('C0Index', 'C1Index', 'C2Index', "C3Index").write.format('com.databricks.spark.csv').save(HADOOPDIR+"data/click_fraud/extraction/publishers_23feb12.csv")


#mapClickCategoricalFeatures()
mapPublisherCategoricalFeatures()


