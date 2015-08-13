import numpy as np
from pylab import *
import sys
from pyspark import SparkContext, SparkConf

App_Name = "Data visualization"
DEVICES_FILE =  '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/devices.csv'
COOKIES_FILE = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/cookies.csv'
OUT_DIR = '/home/ken/Downloads/ASHISH_Data/DrawbridgeData/images'

def main():
	#Configure Spark
	conf = SparkConf().setAppName(App_Name)
	conf = conf.setMaster("local")
	sc = SparkContext(conf=conf)
	return sc


def plotComputerOSTypes(sc):
	
	cookies = sc.textFile(COOKIES_FILE)
	
	cookie_fields = cookies.map(lambda line: line.split(","))
	
	# persist the field as it is needed again

	cookie_fields.persist()
	
	cookie_count = cookie_fields.map(lambda fields: (fields[2], 1)).reduceByKey(lambda x, y : x+y).collect()
	
	x_axis1 = np.array([c[0] for c in cookie_count])
	y_axis1 = np.array([c[1] for c in cookie_count])
	
	x_axis = x_axis1[np.argsort(y_axis1)]
	y_axis = y_axis1[np.argsort(y_axis1)]

	pos = np.arange(len(x_axis))
	width = 1.0
	
	ax = plt.axes()
	ax.set_xticks(pos + (width / 2))
	ax.set_xticklabels(x_axis)
	ax.set_ylabel('Count')
	ax.set_xlabel('Cookie Types')

	rect = plt.bar(pos, y_axis, width, color='lightblue')
	plt.xticks(rotation=30)
	fig = matplotlib.pyplot.gcf()
	fig.set_size_inches(16, 10)
	
	autoLabels(rect, ax)
	
	savefig(OUT_DIR+"/cookie_compOS_types.png", dpi=None, facecolor='w', edgecolor='w',
        orientation='portrait', papertype=None, format=None,
        transparent=False, bbox_inches=None, pad_inches=0.1,
        frameon=None)

	show()


def plotDeviceTypes(sc):
	
	devices = sc.textFile(DEVICES_FILE)

	device_fields = devices.map(lambda line: line.split(","))
	
	# persist the field as it is needed again

	device_fields.persist()
	
	device_count = device_fields.map(lambda fields: (fields[2], 1)).reduceByKey(lambda x, y : x+y).collect()
	
	x_axis1 = np.array([c[0] for c in device_count])
	y_axis1 = np.array([c[1] for c in device_count])
	
	x_axis = x_axis1[np.argsort(y_axis1)]
	y_axis = y_axis1[np.argsort(y_axis1)]

	pos = np.arange(len(x_axis))
	width = 1.0
	
	ax = plt.axes()
	ax.set_xticks(pos + (width / 2))
	ax.set_xticklabels(x_axis)
	ax.set_ylabel('Count')
	ax.set_xlabel('Device Types')

	rect = plt.bar(pos, y_axis, width, color='lightblue')
	plt.xticks(rotation=30)
	fig = matplotlib.pyplot.gcf()
	fig.set_size_inches(16, 10)
	
	autoLabels(rect, ax)
	
	savefig(OUT_DIR+"/device_types.png", dpi=None, facecolor='w', edgecolor='w',
        orientation='portrait', papertype=None, format=None,
        transparent=False, bbox_inches=None, pad_inches=0.1,
        frameon=None)

	show()

	
def autoLabels(rects, ax):
	
	for rect in rects:
		h = rect.get_height()
		ax.text (rect.get_x() + rect.get_width()/2., 1.05 * h, '%d' % int(h), ha='center', va='bottom')


spark_context = main()
plotDeviceTypes(spark_context)
plotComputerOSTypes(spark_context)
