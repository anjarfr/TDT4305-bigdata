from rdd import RDD


# Make RDD of artists, map year of birth and find min value.
year = RDD("./datasets/artists.csv").map(lambda line: line.split(",")[4]).min()

print("The oldest artist was born in {}.".format(year))