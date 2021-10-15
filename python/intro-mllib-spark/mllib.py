from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
import numpy as np
import time
import os
import sys

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ['PYTHONPATH'] = ':'.join(sys.path)

current_dir = os.getcwd()

sc = SparkContext(appName="MNISTDigitsDT")
fileNameTrain = 'mnist_train.csv'
fileNameTest = 'mnist_test.csv'
mnist_train = sc.textFile(os.path.join(current_dir, fileNameTrain))
mnist_test = sc.textFile(os.path.join(current_dir, fileNameTest))

def parsePoint(line):
  #Parse a line of text into an MLlib LabeledPoint object
  values = line.split(',')
  values = [0 if e == '' else int(e) for e in values]
  return LabeledPoint(int(values[0]), values[1:])

#skip header
header = mnist_train.first() #extract header
mnist_train = mnist_train.filter(lambda x:x !=header) 
# filter out header using a lambda
print("Skipped header:")
print(mnist_train.first())

labeledPoints = mnist_train.map(parsePoint)
# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = labeledPoints.randomSplit([0.7, 0.3])
print("After .map(parsePoint")
print(mnist_train.first())

depthLevel = 4
treeLevel = 3
numClasses = 10
maxBins = 32
#start timert
start_time = time.time()
#this is building a model using the Random Forest algorithm from Spark MLLib
model = RandomForest.trainClassifier(trainingData, numClasses=numClasses, categoricalFeaturesInfo={}, numTrees=treeLevel, featureSubsetStrategy="auto", impurity='gini', maxDepth=depthLevel, maxBins=maxBins) 
print("Training time --- %s seconds ---" % (time.time() - start_time))

# Evaluate model on test instances and compute test error
# start timer
start_time = time.time()
# make predictions using the Machine Learning model created prior
predictions = model.predict(testData.map(lambda x: x.features))
# validate predictions using the training set
labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
testErr = labelsAndPredictions.filter(lambda v : v[0] != v[1]).count() / float(testData.count())
print('Test Error = ' + str(testErr))
print("Prediction time --- %s seconds ---" % (time.time() - start_time))
print(labelsAndPredictions.first())
# print('Learned classification tree model:')
print(model.toDebugString())