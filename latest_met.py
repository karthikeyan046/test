import time
import json
from pyspark.sql import SparkSession


def monitor_job_status(spark_context, interval=5):
    """
    Monitor job and stage progress using statusTracker.
    
    Args:
        spark_context: SparkContext instance.
        interval: Polling interval in seconds.
    """
    status_tracker = spark_context.statusTracker()
    job_metrics = []

    while True:
        active_jobs = status_tracker.getActiveJobsIds()
        if not active_jobs:
            print("No active jobs. Monitoring completed.")
            break

        for job_id in active_jobs:
            job_info = status_tracker.getJobInfo(job_id)
            if job_info:
                print(f"Job {job_id}: Status={job_info.status()} | NumTasks={job_info.numTasks()}")
                job_metrics.append({
                    "jobId": job_id,
                    "status": job_info.status(),
                    "numTasks": job_info.numTasks()
                })

        time.sleep(interval)

    return job_metrics


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("PySpark Job Monitoring") \
        .getOrCreate()

    # Example transformation
    input_file_path = "gs://<your-bucket-name>/example.csv"
    output_file_path = "gs://<your-bucket-name>/example_processed.parquet"

    try:
        df = spark.read.csv(input_file_path, header=True, inferSchema=True)
        df = df.withColumn("processed", lit("yes"))
        df.write.mode("overwrite").parquet(output_file_path)
        print(f"File processed successfully: {output_file_path}")
    except Exception as e:
        print(f"Error processing file: {e}")

    # Monitor job statuses
    metrics = monitor_job_status(spark.sparkContext)
    print("Job Metrics:", metrics)

    # Save metrics to file
    metrics_file_path = "job_metrics.json"  # Local file path for demonstration
    with open(metrics_file_path, 'w') as f:
        json.dump(metrics, f, indent=4)
    print(f"Metrics saved to {metrics_file_path}")
