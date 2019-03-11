from rdd import RDD

count = RDD("./datasets/albums.csv").map(lambda line: line.split(",")[3]).distinct().count()
print("There are {} distinct genres in albums.csv.".format(count))