import sys
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('App1').setMaster("local")
sc = SparkContext(conf=conf)

cookies = sc.textFile('/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv')

cookie_fields = cookies.map(lambda line: line.split(','))

# persist the field as it is needed again

cookie_fields.persist()

cookies_train = cookie_fields.filter(lambda field: "-1" not in field[0])

file = "/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies_train"

cookies_train.saveAsTextFile(file)
