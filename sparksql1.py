from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import lit

# Custom Listener
class JobMetricsListener:
    def __init__(self):
        self.job_metrics = []

    def on_job_start(self, job_start_info):
        print(f"Job {job_start_info.jobId} started at {job_start_info.time}")
        self.job_metrics.append({
            "jobId": job_start_info.jobId,
            "start_time": job_start_info.time,
        })

    def on_job_end(self, job_end_info):
        print(f"Job {job_end_info.jobId} ended with result: {job_end_info.jobResult}")
        for job in self.job_metrics:
            if job["jobId"] == job_end_info.jobId:
                job["end_time"] = job_end_info.time
                job["result"] = job_end_info.jobResult
                break

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Spark Listener Example") \
    .getOrCreate()

# Attach Listener
listener = JobMetricsListener()
spark.sparkContext.addSparkListener(listener)

# Run Spark SQL Query
data = [("Alice", 29), ("Bob", 35), ("Cathy", 32)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.createOrReplaceTempView("people")

query = "SELECT Name, Age FROM people WHERE Age > 30"
result_df = spark.sql(query)
result_df.show()

# Display captured metrics
print("Captured Metrics:", listener.job_metrics)
