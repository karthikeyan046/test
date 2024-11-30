from pyspark.sql import SparkSession
import time

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SQL Metrics Example") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

# Example dataset
data = [(1, "Alice", 29), (2, "Bob", 35), (3, "Cathy", 32)]
columns = ["ID", "Name", "Age"]
df = spark.createDataFrame(data, columns)

# Register the DataFrame as a SQL temporary view
df.createOrReplaceTempView("people")

# Function to capture SQL query execution metrics
def capture_sql_metrics(sql_query):
    try:
        # Get SparkContext and StatusTracker
        sc = spark.sparkContext
        status_tracker = sc.statusTracker()

        # Get start time
        start_time = time.time()

        # Execute the SQL query
        result = spark.sql(sql_query)
        result.collect()  # Trigger query execution

        # Get end time
        end_time = time.time()

        # Get all active stages
        active_stage_ids = status_tracker.getActiveStageIds()

        # Get completed stages
        completed_stages = status_tracker.getCompletedStageIds()

        # Get current job IDs (may be empty if jobs finished too quickly)
        active_jobs = status_tracker.getActiveJobIds()

        # Metrics dictionary
        metrics = {
            "execution_time_seconds": end_time - start_time,
            "query": sql_query,
            "active_stages": active_stage_ids,
            "completed_stages": completed_stages,
            "active_jobs": active_jobs,
            "num_rows": result.count(),
            "num_columns": len(result.columns),
        }

        # Show query result (optional)
        print("Query Result:")
        result.show()

        return metrics

    except Exception as e:
        print(f"Error capturing SQL metrics: {e}")
        return {}

# Example SQL query
sql_query = "SELECT Name, Age FROM people WHERE Age > 30"

# Capture metrics
metrics = capture_sql_metrics(sql_query)

# Print captured metrics
print("Captured Metrics:")
print(metrics)
