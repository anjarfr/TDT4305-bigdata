from rdd import RDD

rdd1 = RDD('./datasets/artists.csv')
rdd2 = RDD('./datasets/albums.csv')

def find_name(line):
    if line[2]:
        return line[2]
    return line[1]


# (artist_id, artist_name)
norwegian_artists = rdd1.map(lambda line: line.split(',')).filter(lambda x: x[5] == 'Norway').map(lambda y: (y[0], find_name(y)))

# (artist_id, mtv_review)
albums = rdd2.map(lambda line: line.split(',')).map(lambda x: (x[1], float(x[8])))

# (artist_id, (artist_name, mtv_review))
norwegian_albums = norwegian_artists.join(albums)

# (artist_id, (artist_name, avg_critic))
reduced = norwegian_albums.reduceByKey(lambda x, y: (x[0], (x[1]+y[1])/2))

# Save as TSV file
reduced.map(lambda x: '{name}\tNorway\t{mtv}'.format(name=x[1][0], mtv=x[1][1])).saveAsTextFile("./datasets/result_9")