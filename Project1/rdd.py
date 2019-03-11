import os
from pyspark import SparkConf, SparkContext

def RDD(filePath):
    sc = SparkContext.getOrCreate(SparkConf())
    path = filePath
    RDD = sc.textFile(name=path)
    return RDD

def toTSVLine(data):
  return '\t'.join(str(d) for d in data)