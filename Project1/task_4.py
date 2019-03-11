from rdd import RDD, toTSVLine
from operator import add

rdd = RDD("./datasets/albums.csv")

# map every line to key/value pairs for fast reduce and sorting
albums = rdd.map(lambda line: (int(line.split(',')[1]), 1))
added = albums.reduceByKey(add)
completelysorted = added.sortByKey().sortBy(lambda x: x[1], ascending=False)

# Save to TSV file
lines = completelysorted.map(toTSVLine)
lines.saveAsTextFile('./datasets/result_4')