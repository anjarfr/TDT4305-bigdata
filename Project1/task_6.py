from rdd import RDD
from pyspark import SparkContext, SparkConf



# RDD from text file
rdd = RDD('./datasets/albums.csv')

# Create key/value pairs of (album id, average critic)
critics = rdd.map(lambda line: line.split(',')).map(lambda x: (x[0], (float(x[7])+float(x[8])+float(x[9]))/3))


# sortByKey() sorts alphabetically. sortBy() sorts by number of sales in descending order
sortedreview = critics.sortByKey().sortBy(lambda x: x[1], ascending=False)

# Get the 10 best albums based on avg critic
sc = SparkContext.getOrCreate(SparkConf())
top = sc.parallelize(c=sortedreview.take(10))

# Save as TSV file. set coalesce(1) so that we can use this file in Task 7
top.map(lambda x: '{album}\t{avg}'.format(album=x[0], avg=x[1])).coalesce(1).saveAsTextFile("./datasets/result_6")