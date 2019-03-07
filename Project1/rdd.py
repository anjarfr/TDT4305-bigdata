import os
from pyspark import SparkConf, SparkContext

"""
sc = SparkContext.getOrCreate(SparkConf())

path = "./datasets/testalbums.csv"
albums = sc.textFile(name=path)
#albums.saveAsTextFile('./datasets/newfile.txt')
errorlines = albums.filter(lambda line: "error" in line)
print(errorlines.count())
print(albums.collect())
print(albums.count()) #teller antall linjer
print(albums.take(5)) #printer de 5 føreste linjene

folk_albums = albums.filter(lambda x: "Latino" in x) #teller antall latino-album
print(folk_albums.collect())

#map skjønte jeg ikke så mye av, se nettside i bookmark

#OBS! collect() tar mye tid og må brukes med omhu.

#Sampling RDD
#transformation sample og action takeSample. Skjønner ikke hva
#sample tar som input.
"""

def RDD(filePath):
    sc = SparkContext.getOrCreate(SparkConf())
    path = filePath
    RDD = sc.textFile(name=path)
    return RDD

def toTSVLine(data):
  return '\t'.join(str(d) for d in data)