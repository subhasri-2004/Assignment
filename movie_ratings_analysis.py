from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_unixtime
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("MovieRatingsAnalysis").getOrCreate()

data = [
    (1, 101, 4.5, 1672531200),
    (1, 102, 3.5, 1672531300),
    (2, 101, 5.0, 1672531400),
    (2, 103, 4.0, 1672531500),
    (3, 104, 2.5, 1672531600),
    (3, 102, 3.0, 1672531700),
    (3, 105, 4.0, 1672531800),
    (4, 106, 5.0, 1672531900),
    (4, 107, 4.5, 1672532000),
    (5, 101, 3.5, 1672532100),
    (5, 108, 4.0, 1672532200),
    (6, 102, 4.0, 1672532300),
    (6, 109, 4.5, 1672532400),
    (6, 110, 3.5, 1672532500),
    (6, 111, 2.0, 1672532600),
    (7, 101, 4.0, 1672532700),
    (7, 112, 4.5, 1672532800),
    (7, 113, 3.0, 1672532900),
    (7, 114, 3.5, 1672533000),
    (7, 115, 4.0, 1672533100),
    (7, 116, 5.0, 1672533200)
]

columns = ["MovieID", "UserID", "Rating", "Timestamp"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("Date", from_unixtime(col("Timestamp")).cast("timestamp"))

avg_ratings = df.groupBy("MovieID").agg(avg("Rating").alias("AverageRating"))

user_ratings_count = df.groupBy("UserID").agg(count("MovieID").alias("MovieCount"))
users_more_than_5 = user_ratings_count.filter(col("MovieCount") > 5)

top_movies = avg_ratings.orderBy(col("AverageRating").desc()).limit(5)

user_counts = user_ratings_count.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(user_counts["UserID"], user_counts["MovieCount"], color='skyblue')
plt.xlabel("User ID")
plt.ylabel("Number of Movies Rated")
plt.title("Users vs Number of Movies Rated")
plt.xticks(rotation=45)
plt.show()

df.show()
avg_ratings.show()
users_more_than_5.show()
top_movies.show()
