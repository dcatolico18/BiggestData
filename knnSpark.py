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
###################################################################
""" Given two Sparse Vectors, compute cosine similarity between them"""
def similarity(a,b) : 
    aMagnitude = math.sqrt(float(sum([aVal**2 for aVal in a.values[:-1]])))
    bMagnitude = math.sqrt(float(sum([bVal**2 for bVal in b.values[:-1]])))
    resultNumerator = 0
    for i in range(len(a) - 3):
        resultNumerator += abs(a[i] - b[i])
    resultNumerator  += math.hypot(a[-3] - b[-3], a[-2] - b[-2])
    resultDenominator = aMagnitude*bMagnitude
    if resultDenominator == 0:
        return 0
    return 1.0*resultNumerator/resultDenominator

conf = SparkConf().setAppName("KNN").set("spark.executor.memory", "4g")
sc = SparkContext(conf=conf)

trajectoriesRDD = sc.textFile("newInput")
testRDD = sc.textFile("newShort")



for i in range(200):

    randomTest = testRDD.takeSample(False, 1)
    actualTime = randomTest[-1]
    testVector = randomTest[:-1]

    # From this movie vector, we will randomly select a userId and set their rating to zero.
    # The idea would be to try to predict that rating and see how close we come to the actual value 
    predVecValues = randMovieVector.values
    predVecIndices = randMovieVector.indices
    index = random.randint(0, len(predVecValues)-1 )
    predVecValues[index] = 0 # set rating to zero
    randUserId = predVecIndices[index]
    
    ## 2) compute cosine simularity with "randMovieVector" and each vector in RDD
    # result is a RDD of (cosSimValue, movieId)
    result = sparseRatingRDD.map(lambda v : (v[0], cosineSimilarity(v[1], randMovieVector)))\
              .map(lambda x: (x[1], x[0]))\
              .sortByKey(ascending=False)
     
    movieTitles = [x[1] for x in result.take(100)]

    ## 3) get predicted rating 
    ratingsOfUser = ratingsRDD.filter(lambda x: x[1][0] == randUserId and x[0] in movieTitles)\
                            .map(lambda x: x[1][1])
    sumList = ratingsOfUser.take(5)  # 5 = K 

    actualRating = ratingsRDD.filter(lambda x: int(x[0]) == randMovieId and x[1][0] == randUserId )\
                           .map(lambda x: x[1][1])

    print "Prediction for userId = %d for movieId= %s" % (randUserId, randMovieId)
    print "Predicted Rating:", sum(sumList)/len(sumList), "Actual Rating:", actualRating.take(1)


# clean up
sc.stop()