from Project1.rdd import RDD, toTSVLine
from operator import add

rdd = RDD("./datasets/artists.csv")

countries = rdd.map(lambda line: (''.join(line.split(',')[5]), 1))
count = countries.reduceByKey(add)
sorted = count.sortByKey().sortBy(lambda x: x[1], ascending=False)

print(sorted.collect())

#sorted.map(lambda x: '{country}\t{count}'.format(country=x[0], count=x[1])).coalesce(1).saveAstextFile("./datasets/result_3")

lines = sorted.map(toTSVLine)

lines.saveAsTextFile('./datasets/result_3')
