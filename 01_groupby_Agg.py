from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("DistinctUsersPerActionType") \
    .getOrCreate()

# Load the log file into a DataFrame
logs_df = spark.read.csv("path_to_log_file.txt", header=True, inferSchema=True)

# Group by action_type and count distinct users for each action type
# result_df = logs_df.groupBy("action_type").agg({"user_id": "countDistinct"})
result_df = logs_df.groupBy("action_type").agg({"user_id": "countDistinct"})

# Rename the count column to something more meaningful
result_df = result_df.withColumnRenamed("count(DISTINCT user_id)", "distinct_user_count")

# Show the result
result_df.show()

# Stop the Spark session
spark.stop()
