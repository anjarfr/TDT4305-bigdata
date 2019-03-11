from pyspark import SparkContext, SQLContext
from pyspark.sql.functions import col, countDistinct

sc = SparkContext()
sqlContext = SQLContext(sc)

# Making DF for albums and renaming columns
albums_df = sqlContext.read.csv('./datasets/albums.csv')
oldColumns = albums_df.schema.names
newColumns = ["id", "artist_id", "album_title", "genre", "year_of_pub", "num_of_tracks", "num_of_sales", "rolling_stone_critic", "mtv_critic", "music_maniac_critic"]
albums_df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), albums_df)

# Making DF for artists and renaming columns
artists_df = sqlContext.read.csv('./datasets/artists.csv')
oldColumns = artists_df.schema.names
newColumns = ["id", "real_name", "art_name", "role", "year_of_birth", "country", "city", "email", "zip_code"]
artists_df = reduce(lambda data, idx: data.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), artists_df)

# Number og distinct artists
artists_df.agg(countDistinct(col("id")).alias("count")).show()

# Number of distinct albums
albums_df.agg(countDistinct(col("id")).alias("count")).show()

# Number of distinct genres
albums_df.agg(countDistinct(col("genre")).alias("count")).show()

# Number of distinct countries
artists_df.agg(countDistinct(col("country")).alias("count")).show()

# Minimum year_of_pub
albums_df = albums_df.withColumn("year_of_pub", albums_df["year_of_pub"].cast("int"))
albums_df.groupBy().min("year_of_pub").show()

# Maximum year_of_pub
albums_df.groupBy().max("year_of_pub").show()

# Minimum year_of_birth
artists_df = artists_df.withColumn("year_of_birth", artists_df["year_of_birth"].cast("int"))
artists_df.groupBy().min("year_of_birth").show()

# Maximum year_of_birth
artists_df.groupBy().max("year_of_birth").show()

#artists_df.printSchema()
#artists_df.show(5)
