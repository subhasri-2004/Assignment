from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, regexp_extract, hour
import matplotlib.pyplot as plt

spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

log_data = [
    "2024-12-18 10:15:32 INFO User logged in",
    "2024-12-18 10:16:02 ERROR Page not found",
    "2024-12-18 10:17:20 INFO Data uploaded successfully",
    "2024-12-18 11:05:45 WARNING Disk space low",
    "2024-12-18 11:10:30 ERROR Connection timeout",
    "2024-12-18 12:00:15 INFO File deleted",
    "2024-12-18 12:30:45 ERROR Unauthorized access",
    "2024-12-18 13:15:55 INFO User logged out",
    "2024-12-18 13:45:20 WARNING High memory usage",
    "2024-12-18 14:10:10 ERROR Server crash"
]

log_rdd = spark.sparkContext.parallelize(log_data)
log_df = spark.createDataFrame(log_rdd.map(lambda x: (x,)), ["raw_log"])

timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"
level_pattern = r"(INFO|ERROR|WARNING)"
message_pattern = r"(INFO|ERROR|WARNING) (.+)"

log_df = log_df.withColumn("Timestamp", regexp_extract(col("raw_log"), timestamp_pattern, 1))
log_df = log_df.withColumn("LogLevel", regexp_extract(col("raw_log"), level_pattern, 1))
log_df = log_df.withColumn("Message", regexp_extract(col("raw_log"), message_pattern, 2))

log_level_counts = log_df.groupBy("LogLevel").agg(count("LogLevel").alias("Count"))
error_logs = log_df.filter(col("LogLevel") == "ERROR")

log_df = log_df.withColumn("Hour", hour(col("Timestamp").cast("timestamp")))
logs_per_hour = log_df.groupBy("Hour").agg(count("raw_log").alias("LogCount"))

logs_per_hour_pd = logs_per_hour.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(logs_per_hour_pd["Hour"], logs_per_hour_pd["LogCount"], color='blue')
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Logs")
plt.title("Logs per Hour")
plt.xticks(range(24))
plt.show()

log_df.show()
log_level_counts.show()
error_logs.show()
logs_per_hour.show()
