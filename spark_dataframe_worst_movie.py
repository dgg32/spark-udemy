## worstmovie.py, movie.csv, ratings csv first transfer through:
#scp -P 2222 '/home/sih13/Downloads/ratings.csv' maria_dev@127.0.0.1:/home/maria_dev/ratings.csv

## then distribute it to hdfs
#  hadoop fs -put ratings.csv /user/maria_dev/ratings.csv

#https://stackoverflow.com/questions/42091575/pyspark-load-file-path-does-not-exist


from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("worstMove").getOrCreate()

name_file = "/user/maria_dev/movies.csv"

#movieNames = spark.sparkContext.textFile(name_file)

movieNames = spark.read.option("header", "true").option("inferSchema", "true").csv(name_file)

input_file = "/user/maria_dev/ratings.csv"
movies = spark.read.option("header", "true").option("inferSchema", "true").csv(input_file)

ratings = movies.select("movieId", "rating")

from pyspark.sql import functions as F

groupMovies = ratings.groupBy('movieId').agg(F.mean('rating'), F.count('rating'))

top_20_worst_movies = groupMovies.filter(groupMovies["count(rating)"] > 9)

top_20_worst_movies.join(movieNames, on="movieId").orderBy('avg(rating)', ascending=True).select('title', "avg(rating)", "count(rating)", "genres").show()

spark.stop()
