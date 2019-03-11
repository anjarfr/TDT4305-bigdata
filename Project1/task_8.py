from rdd import RDD, toTSVLine

def find_name(line):
    if line[2]:
        return line[2]
    return line[1]


albums = RDD('./datasets/albums.csv').map(lambda line: line.split(',')).filter(lambda album: album[8] == "5")
albums = albums.map(lambda col: (col[1], col[8]))

artists = RDD('./datasets/artists.csv').map(lambda line: line.split(',')).map(lambda col: (col[0], find_name(col)))

artists_MTV_5 = albums.join(artists).map(lambda line: line[1][1]).distinct().sortBy(lambda x: x[0], ascending=True)

artists_MTV_5.map(lambda x: '{name}'.format(name=x.encode('utf-8'))).saveAsTextFile("./datasets/result_8")

