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

def parseInput(line):
    """ Parses a rating record in MovieLens format userId::movieId::rating::timestamp ."""
    # print line
    if type(line) != list:
        fields = line.split(",")
        return map(float, fields)
    else:
        return []


###################################################################
""" Given two Sparse Vectors, compute cosine similarity between them"""
def similarity(a,b) : 
    # print a, b
    # print "CHECKINGGGG"
    # aMagnitude = math.sqrt(float(sum([aVal**2 for aVal in a[:-1]])))
    # bMagnitude = math.sqrt(float(sum([bVal**2 for bVal in b[:-1]])))
    dist = 0
    hugeeeeNumb = 1000

    # for i in range(len(a) - 3):
    #     dist += abs(a[i] - b[i])
    
    if a[0:6] != b[0:6]:
        return hugeeeeNumb
    elif sum(a[6:11]) != sum(b[6:11]):
        dist+= 7
    dist += 5*(a[-4] - b[-4])
    dist += 5*(math.hypot(a[-3] - b[-3], a[-2] - b[-2]))
    # resultDenominator = aMagnitude*bMagnitude
    # if resultDenominator == 0:
    #     return 0
    # return 1.0*dist/resultDenominator

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
    # print(randomTest, actualTime, testVector)

    # From this movie vector, we will randomly select a userId and set their rating to zero.
    # The idea would be to try to predict that rating and see how close we come to the actual value 
    # predVecValues = randMovieVector.values
    # predVecIndices = randMovieVector.indices
    # index = random.randint(0, len(predVecValues)-1 )
    # predVecValues[index] = 0 # set rating to zero
    # randUserId = predVecIndices[index]
    
    ## 2) compute cosine simularity with "randMovieVector" and each vector in RDD
    # result is a RDD of (cosSimValue, movieId)
    result = trajectoriesRDD.map(lambda v : (squareSim(v, testVector), v[-1]))\
        .sortByKey(ascending=True)
     

    # movieTitles = [x[1] for x in result.take(100)]
    # print result.take(100)


    
    k = 200
    average = 1.0*sum(result.map(lambda x: x[1]).take(k))/k
    # for x in result.take(k):
    #     sum += x[1]
        # print x

    print abs(average- actualTime)
    if (abs(average- actualTime) <= 15):
        correctCount += 1
    # print "average", average
    # print "actualTime", actualTime

    totalCount += 1

print "got right !!!", correctCount*1.0/totalCount

    # ## 3) get predicted rating 
    # ratingsOfUser = ratingsRDD.filter(lambda x: x[1][0] == randUserId and x[0] in movieTitles)\
    #                         .map(lambda x: x[1][1])
    # sumList = ratingsOfUser.take(5)  # 5 = K 

    # actualRating = ratingsRDD.filter(lambda x: int(x[0]) == randMovieId and x[1][0] == randUserId )\
    #                        .map(lambda x: x[1][1])

    # print "Prediction for userId = %d for movieId= %s" % (randUserId, randMovieId)
    # print "Predicted Rating:", sum(sumList)/len(sumList), "Actual Rating:", actualRating.take(1)


# clean up
sc.stop()