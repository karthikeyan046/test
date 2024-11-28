Here's an updated code snippet that writes the job metrics to a text file instead of a JSON file:

```
from pyspark.sql import SparkSession

Create SparkSession
spark = SparkSession.builder.appName("Job Metrics").getOrCreate()

Get MetricsSystem
metrics_system = spark._sc._jvm.org.apache.spark.metrics.MetricsSystem

Define a function to capture job metrics
def capture_job_metrics():
    # Get job metrics
    job_metrics = metrics_system.getJobMetrics()

    # Extract relevant metrics
    metrics_data = []
    for metric in job_metrics:
        metrics_data.append(f"{metric.name()}: {metric.value()}")

    # Save metrics to a text file
    with open("job_metrics.txt", "w") as f:
        for metric in metrics_data:
            f.write(metric + "\n")

Run a sample Spark job
data = [("John", 25), ("Jane", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])
df.show()

Capture job metrics
capture_job_metrics()

Stop SparkSession
spark.stop()
```

This code will write the job metrics to a text file named `job_metrics.txt` in the current working directory.

Here's an example of what the `job_metrics.txt` file might look like:

```
executorCpuTime: 100
executorDeserializeCpuTime: 50
executorDeserializeTime: 200
executorInputBytes: 1000
executorInputRecords: 100
executorJvmHeapBytes: 1024
executorJvmOffHeapBytes: 512
executorMemoryBytesSpilled: 0
executorOnHeapMemoryUsedBytes: 512
executorOnHeapPeakMemoryUsedBytes: 1024
executorOutputBytes: 1000
executorOutputRecords: 100
executorRunTime: 1000
executorShuffleBytesRead: 1000
executorShuffleBytesWritten: 1000
executorShuffleRecordsRead: 100
executorShuffleRecordsWritten: 100
```

Let me know if you have any other questions!
