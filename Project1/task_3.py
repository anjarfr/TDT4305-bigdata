from rdd import RDD

RDD_country_count = RDD("./datasets/artists.csv")
grp = RDD_country_count.groupBy("country").count(1)
print(grp.filter(lambda grp : "in grp))
