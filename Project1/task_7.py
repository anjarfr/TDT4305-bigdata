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

# Join key/value pairs on album_id as key. Output is (album_id, (artist_id, avg))
criticsjoined = artist_ids.join(critics)

# Flip position of album_id and artist_id so that artist_id is key: (artist_id, (album_id, avg))
flip_ids = criticsjoined.map(lambda line: (line[1][0], (line[0], line[1][1])))
# Join on artist_id. Output: (artist_id, ((album_id, avg), country))
fulljoin = flip_ids.join(country)

# Save to TSV file
fulljoin.map(lambda line: '{}\t{}\t{}'.format(line[1][0][0], line[1][0][1], line[1][1])).coalesce(1).saveAsTextFile("./datasets/result_7/")