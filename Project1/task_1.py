from rdd import RDD

count = RDD("./datasets/albums.csv").map(lambda line: line.split(",")[3]).distinct().count()

print("We have {} distinct genres in albums.csv.".format(count))