from Project1.rdd import RDD, toTSVLine
from pyspark import SparkContext, SparkConf



# RDD from text file
rdd = RDD('./datasets/albums.csv')


# Create key/value pairs of album id and average critic
def avg_critic(rs, mtv, mm):
    return (float(rs)+float(mtv)+float(mm))/3


def splitline(x):
    line = x.split(',')
    album_id = line[0]
    rs = line[7]
    mtv = line[8]
    mm = line[9]
    tuple = (album_id, avg_critic(rs, mtv, mm))
    return tuple


critics = rdd.map(lambda line: (splitline(line)))

# sortByKey() sorts alphabetically. sortBy() sorts by number of sales in descending order
sortedreview = critics.sortByKey().sortBy(lambda x: x[1], ascending=False)

sc = SparkContext.getOrCreate(SparkConf())
top = sc.parallelize(c=sortedreview.take(10))

# Save as TSV file. set coalesce(1) so that we can use this file in Task 7
top.map(lambda x: '{album}\t{avg}'.format(album=x[0], avg=x[1])).coalesce(1).saveAsTextFile("./datasets/result_6")
