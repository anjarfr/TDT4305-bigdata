from Project1.rdd import RDD
import csv
from pyspark import SparkContext as sc

rdd = RDD("./datasets/testartists.csv")

countries = rdd.map(lambda line: (''.join(line.split(',')[5]), 1))
count = countries.reduceByKey(lambda x, y: x + y)
sorted = count.sortByKey().sortBy(lambda x: x[1], ascending=False)

print(sorted.collect())

def toTSVLine(data):
  return '\t'.join(str(d) for d in data)

lines = sorted.map(toTSVLine)

lines.saveAsTextFile('./datasets/result_3.csv')
