from pyspark import SparkConf, SparkContext
from numpy import array
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from time import time
import pickle
import sys

DATA_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/Extraction/complete_train2'
TEST_FILE = '/home/ashish/Downloads/ASHISH_Data/DrawbridgeData/Extraction/test_5336'

parsed_test_data = ""

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

def testDataMapper(line):

	feats = line.split(",")
	
	test_feats = feats[1:3]
	test_feats.insert(2, feats[4])
	test_feats.insert(3, feats[5])
	test_feats.insert(4, feats[6])
	test_feats = array([float(feature) for feature in test_feats])
	return test_feats
	#use this in case you need to return LabeledPoint	
	#label = 0	
	#return LabeledPoint(float(label), test_feats)


def predictor(sc):

	test_data = sc.textFile(TEST_FILE)
	test_data.persist()
	global parsed_test_data
	parsed_test_data = test_data.map(testDataMapper)
	print parsed_test_data.take(10)
		

def mapper(line):
	# this mapper converts input line to a feature vector

	feats = line.split(",")
	#LRSGD takes label as the first(index 0) input
	label = feats[len(feats) - 1] 
	#take features from index 3 excluding last
	feats = feats[3:len(feats) - 1]
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
	model = LogisticRegressionWithSGD.train(parsedData)
	t1 = time() - t0
	print "Classifier trained in {} seconds".format(round(t1, 3))

	labelsAndPreds = parsedData.map(lambda point: (point.label, model.predict(point.features)))
		

	# Evaluating the model on training data
	trainErr = labelsAndPreds.filter(lambda (v, p): v == p).count() / float(parsedData.count())

	# Print some stuff
	print("Training Error = " + str(trainErr))
	
	print "*************************** TESTING NOW ***********************"
	
	preds = parsed_test_data.map(lambda point: model.predict(point))
	
	with open('/home/ashish/Desktop/preds.pickle', 'wb') as f:
		pickle.dump(preds.collect(), f)			
	
	
	#this returns prediction output as pair of label and value	
	#print preds.collect()
	#labels_Preds = parsed_test_data.map(lambda point: (point.label, model.predict(point.features)))
	#print labels_Preds.collect()	
	#test_accuracy = labels_Preds.filter(lambda (v, p): v == p).count() / float(parsed_test_data.count())
	#print "Test accuracy is {}".format(round(test_accuracy, 3))	


def postProcess(sc):
	
	test_data = sc.textFile(TEST_FILE) 
	test_fields = test_data.map(lambda line: line.split(',')).collect()

	with open('/home/ashish/Desktop/preds.pickle', 'rb') as f:
		preds = pickle.load(f)
	
	f = open('/home/ashish/Desktop/out.txt', 'w')
	sys.stdout = f
	i = 0
	for t in test_fields:
		t.insert(7, preds[i])
		i = i + 1
		for item in t:
			print str(item),
		print "\n"
	f.close()	

spark_context = getSparkContext()
#predictor(spark_context)
#processData(spark_context)
postProcess(spark_context)
