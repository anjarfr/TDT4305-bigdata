from Project1.rdd import RDD, toTSVLine
from pyspark import SparkContext, SparkConf



# RDD from text file
rdd1 = RDD('./datasets/result_6/part-00000')
rdd2 = RDD('./datasets/albums.csv')
rdd3 = RDD('./datasets/artists.csv')

# Create key/value pairs of (album id, avg critic)
critics = rdd1.map(lambda line:  tuple(line.split('\t')))
# Create key/value pairs of (album id, artist id)
artist_ids = rdd2.map(lambda line: line.split(',')).map(lambda col: (col[0], col[1]))
# Create key/value pairs of (artist id, country)
country = rdd3.map(lambda line: line.split(',')).map(lambda col: (col[0], col[5]))


criticsjoined = critics.join(artist_ids)
countryjoined = artist_ids.join(country)


# Save to TSV file
#top.map(lambda y: '{var1}\t{var2}'.format(var1=y[0], var2=y[1])).coalesce(1).saveAstextFile("./datasets/result_7/")


# lines = top.map(toTSVLine)
# lines.saveAsTextFile('./datasets/result_6')