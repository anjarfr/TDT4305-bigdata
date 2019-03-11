from rdd import RDD, toTSVLine
from operator import add


# RDD from text file
rdd = RDD('./datasets/albums.csv')

# Create key/value pairs of (genre, tracks sold). List is alreay sorted by id, thus we don't need id
genres = rdd.map(lambda line:  (''.join(line.split(',')[3]), int(line.split(',')[6])))

# Aggregate all genres and sum salesnumbers
sortbysales = genres.reduceByKey(add)

# sortByKey() sorts alphabetically. sortBy() sorts by number of sales in descending order
completed = sortbysales.sortByKey().sortBy(lambda x: x[1], ascending=False)

# Save to TSV file
lines = completed.map(toTSVLine)
lines.saveAsTextFile('./datasets/result_5')