from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, from_unixtime, unix_timestamp, hour
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("TripDataAnalysis").getOrCreate()

data = [
    (1, "2025-03-08 08:00:00", "2025-03-08 08:30:00", 12.5, 25.0),
    (2, "2025-03-08 09:15:00", "2025-03-08 09:45:00", 8.0, 18.0),
    (3, "2025-03-08 10:05:00", "2025-03-08 10:50:00", 15.0, 30.0),
    (4, "2025-03-08 11:20:00", "2025-03-08 12:10:00", 20.0, 40.0),
    (5, "2025-03-08 12:45:00", "2025-03-08 13:15:00", 5.5, 12.0),
    (6, "2025-03-08 14:00:00", "2025-03-08 14:50:00", 18.0, 36.0),
    (7, "2025-03-08 15:10:00", "2025-03-08 15:40:00", 9.0, 20.0),
    (8, "2025-03-08 16:25:00", "2025-03-08 16:55:00", 7.0, 15.0),
    (9, "2025-03-08 17:30:00", "2025-03-08 18:15:00", 22.0, 50.0),
    (10, "2025-03-08 19:00:00", "2025-03-08 19:30:00", 10.0, 22.0)
]

columns = ["TripID", "StartTime", "EndTime", "Distance", "Fare"]

df = spark.createDataFrame(data, columns)

df = df.withColumn("StartTime", col("StartTime").cast("timestamp"))
df = df.withColumn("EndTime", col("EndTime").cast("timestamp"))

df = df.withColumn("TripDuration", (unix_timestamp(col("EndTime")) - unix_timestamp(col("StartTime"))) / 60)

avg_fare_per_mile = df.withColumn("FarePerMile", col("Fare") / col("Distance"))

longest_trips = df.orderBy(col("Distance").desc()).limit(3)

df = df.withColumn("Hour", hour(col("StartTime")))
trips_per_hour = df.groupBy("Hour").agg(count("TripID").alias("TripCount"))

trip_counts = trips_per_hour.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(trip_counts["Hour"], trip_counts["TripCount"], color='orange')
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Trips")
plt.title("Trips per Hour")
plt.xticks(range(24))
plt.show()

df.show()
avg_fare_per_mile.show()
longest_trips.show()
trips_per_hour.show()
