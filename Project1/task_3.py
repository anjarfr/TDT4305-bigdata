from Project1.rdd import RDD, toTSVLine
from operator import add

rdd = RDD("./datasets/artists.csv")

# map every line to key/value pairs for fast reduce and sorting
countries = rdd.map(lambda line: (''.join(line.split(',')[5]), 1))
count = countries.reduceByKey(add)
completed = count.sortByKey().sortBy(lambda x: x[1], ascending=False)

#sorted.map(lambda x: '{country}\t{count}'.format(country=x[0], count=x[1])).coalesce(1).saveAstextFile("./datasets/result_3")

# Save to TSV file
lines = completed.map(toTSVLine)
lines.saveAsTextFile('./datasets/result_3')
