from Project1.rdd import RDD, toTSVLine
from operator import add

rdd = RDD("./datasets/albums.csv")

# map every line to key/value pairs for fast reduce and sorting
albums = rdd.map(lambda line: (int(line.split(',')[1]), 1))
added = albums.reduceByKey(add)
completelysorted = added.sortByKey().sortBy(lambda x: x[1], ascending=False)

#sorted.map(lambda x: '{country}\t{count}'.format(country=x[0], count=x[1])).coalesce(1).saveAstextFile("./datasets/result_3")

# Save to TSV file
lines2 = completelysorted.map(toTSVLine)
lines2.saveAsTextFile('./datasets/result_4')