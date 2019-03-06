from Project1.rdd import RDD

year = RDD("./datasets/artists.csv").map(lambda line: line.split(",")[4]).min()

print("The oldest artist is born in {}.".format(year))