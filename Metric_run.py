


from pyspark.sql import SparkSession

#Create SparkSession
spark = SparkSession.builder.appName("Job Metrics").getOrCreate()

#Get MetricsSystem
metrics_system = spark._sc._jvm.org.apache.spark.metrics.MetricsSystem

#Define a function to capture job metrics
def capture_job_metrics():
    # Get job metrics
    job_metrics = metrics_system.getJobMetrics()

    # Extract relevant metrics
    metrics_data = []
    for metric in job_metrics:
        metrics_data.append(f"{metric.name()}: {metric.value()}")

    # Save metrics to a text file
    #with open("job_metrics.txt", "w") as f:
        for metric in metrics_data:
            #f.write(metric + "\n")
            print(metric)

#Run a sample Spark job
data = [("John", 25), ("Jane", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

#Capture job metrics
capture_job_metrics()

