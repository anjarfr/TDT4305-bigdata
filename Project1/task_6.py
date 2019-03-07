from Project1.rdd import RDD, toTSVLine
from pyspark import SparkContext, SparkConf
def avg_critic(rs, mtv, mm):
    return (float(rs)+float(mtv)+float(mm))/3


# RDD from text file
rdd = RDD('./datasets/albums.csv')

# Create key/value pairs of genre and tracks sold. List is alreay sorted by id, thus we don't need id
critics = rdd.map(lambda line:  (line.split(',')[0], avg_critic(line.split(',')[7], line.split(',')[8], line.split(',')[9])))

# sortByKey() sorts alphabetically. sortBy() sorts by number of sales in descending order
sortedreview = critics.sortByKey().sortBy(lambda x: x[1], ascending=False)

sc = SparkContext.getOrCreate(SparkConf())
top = sc.parallelize(c=sortedreview.take(10))


# Save to TSV file
lines = top.map(toTSVLine)
lines.saveAsTextFile('./datasets/result_6')