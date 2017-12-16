#!/usr/bin/env/python

import sys
import math
import random
import pyspark.mllib.linalg
from operator import add
from os.path import join, isfile, dirname
from pyspark.mllib.linalg import SparseVector, Vectors
from pyspark import SparkConf, SparkContext


###################################################################
""" Given two Sparse Vectors, compute cosine similarity between them"""
def similarity(a,b) : 
    dist = 0
    largeDist = 1000
    
    if a[0:6] != b[0:6]:
        return largeDist
    elif sum(a[6:11]) != sum(b[6:11]):
        dist+= 7
    dist += 5*(a[-4] - b[-4])
    dist += 5*(math.hypot(a[-3] - b[-3], a[-2] - b[-2]))

    return dist

def squareSim(a,b):
    dist = 0
    if a[0:6] != b[0:6]:
        return 1000
    for i in range(len(a[:-1])):
        dist += (a[i] - b[i])**2

    return math.sqrt(dist)


conf = SparkConf().setAppName("KNN").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

trajectories = sc.textFile("newInput.txt")
test = sc.textFile("newShort.txt")
trajectoriesRDD = trajectories.map(parseInput)
testRDD = test.map(parseInput)


###################################################################


correctCount = 0
totalCount = 0
for i in range(50): # 200 

    randomTest = testRDD.takeSample(False, 1)
    actualTime = randomTest[0][-1]
    testVector = randomTest[0]

    result = trajectoriesRDD.map(lambda v : (squareSim(v, testVector), v[-1]))\
        .sortByKey(ascending=True)
    
    k = 200
    average = 1.0*sum(result.map(lambda x: x[1]).take(k))/k

    print abs(average- actualTime)
    if (abs(average- actualTime) <= 15):
        correctCount += 1

    totalCount += 1

print "got right !!!", correctCount*1.0/totalCount

sc.stop()
