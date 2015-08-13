from pyspark import SparkConf, SparkContext
from numpy import array
from pyspark.mllib.classification import LogisticRegressionWithSGD, LogisticRegressionWithLBFGS
from pyspark.mllib.regression import LabeledPoint
from time import time

DATA_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/Extraction/device_cookie_3'

def getSparkContext():

	conf = (SparkConf()
		.setMaster('local[*]')
		.setAppName('LogisticRegression')
		.set("spark.executor.memory", "1g"))
	
	sc = SparkContext(conf = conf)
	return sc


'''
The input format expected in the LogisticRegression algorithm implementation in Spark is a numpy array, in which the first element is the class ( 0 or 1 ), and all elements after the first elements are the features, which must a float datatype.
'''

def mapper(line):
	# this mapper converts input line to a feature vector

	feats = line.split(",")
	#LRSGD takes label as the first(index 0) input
	label = feats[1] 
	#take features from index 3 excluding last
	feats = feats[2:len(feats) - 1]
	#feats.insert(0, label)
	#features = [ float(feature) for feature in feats] #float input is needed
	#return np.array(features)
	return LabeledPoint(float(label), array([float(feature) for feature in feats]))

def processData(sc):
	#load and parse the data
	raw_data = sc.textFile(DATA_FILE)
	raw_data.persist()	
	
	print "Train data size {}".format(raw_data.count()) 
	# map data to a format needed for logistic regression
	parsedData = raw_data.map(mapper)
	
	print "Sample of input to algorithm ", parsedData.take(10)
	
	# Train model
	t0 = time()	
	model = LogisticRegressionWithLBFGS.train(parsedData)
	t1 = time() - t0
	print "Classifier trained in {} seconds".format(round(t1, 3))

	labelsAndPreds = parsedData.map(lambda point: (point.label, model.predict(point.features)))
	
	# Evaluating the model on training data
	trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(parsedData.count())

	# Print some stuff
	print("Training Error = " + str(trainErr))

spark_context = getSparkContext()
processData(spark_context)
	
