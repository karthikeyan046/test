from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Spark SQL Metrics Example") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

# Example dataset
data = [(1, "Alice", 29), (2, "Bob", 35), (3, "Cathy", 32)]
columns = ["ID", "Name", "Age"]
df = spark.createDataFrame(data, columns)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# Initialize a dictionary to store metrics
query_metrics = {}

# Function to capture SQL query execution metrics
def capture_sql_metrics(spark_context, sql_query):
    # Get start time
    start_time = time.time()

    # Run the SQL query
    result = spark.sql(sql_query)

    # Trigger query execution
    result.collect()

    # Get end time
    end_time = time.time()

    # Use StatusTracker to get metrics
    status_tracker = spark_context.statusTracker()

    active_jobs = status_tracker.getActiveJobsIds()
    completed_jobs = status_tracker.getCompletedJobIds()

    # Record metrics
    query_metrics["execution_time"] = end_time - start_time
    query_metrics["active_jobs"] = active_jobs
    query_metrics["completed_jobs"] = completed_jobs

    print("Query Result:")
    result.show()

    return query_metrics

# Example SQL Query
sql_query = "SELECT Name, Age FROM people WHERE Age > 30"

# Capture metrics
metrics = capture_sql_metrics(spark.sparkContext, sql_query)

# Print captured metrics
print("Captured Metrics:")
print(metrics)


