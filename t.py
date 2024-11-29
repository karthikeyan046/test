import os
import json
import threading
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from google.cloud import storage


def save_to_gcs(bucket_name, destination_blob_name, local_file_path):
    """
    Uploads a local file to a Google Cloud Storage bucket.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")


def read_and_transform_pyspark(file_path, output_path, spark):
    """
    Reads a file using PySpark, transforms it, and writes the output to Parquet.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn("processed", lit("yes"))
    df.write.mode("overwrite").parquet(output_path)
    print(f"Processed file saved to: {output_path}")


def monitor_job_status(spark_context, job_metrics, stop_event, interval=5):
    """
    Monitor Spark job and stage progress using StatusTracker.
    """
    status_tracker = spark_context.statusTracker()
    
    while not stop_event.is_set():
        active_jobs = status_tracker.getActiveJobsIds()
        if active_jobs:
            for job_id in active_jobs:
                job_info = status_tracker.getJobInfo(job_id)
                if job_info:
                    print(f"Job {job_id}: Status={job_info.status()} | NumTasks={job_info.numTasks()}")
                    job_metrics.append({
                        "jobId": job_id,
                        "status": job_info.status(),
                        "numTasks": job_info.numTasks(),
                        "time": time.time()
                    })
        time.sleep(interval)


if __name__ == "__main__":
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("GCS Metrics Logging") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "1") \
        .config("spark.driver.memory", "1g") \
        .config("spark.yarn.executor.memoryOverhead", "512m") \
        .getOrCreate()

    # File paths
    input_file_path = "gs://<your-bucket-name>/example.csv"  # Replace with your GCS input path
    output_file_path = "gs://<your-bucket-name>/example_processed.parquet"  # Replace with your GCS output path
    local_metrics_path = "job_metrics.json"  # Local temporary file for metrics
    gcs_metrics_path = "metrics/job_metrics.json"  # GCS destination path for metrics

    # Initialize shared data for monitoring
    job_metrics = []
    stop_event = threading.Event()

    try:
        # Start the job monitoring thread
        monitor_thread = threading.Thread(
            target=monitor_job_status,
            args=(spark.sparkContext, job_metrics, stop_event)
        )
        monitor_thread.start()

        # Process the input file
        read_and_transform_pyspark(input_file_path, output_file_path, spark)

        # Signal the monitoring thread to stop and wait for it to finish
        stop_event.set()
        monitor_thread.join()

        # Save metrics locally
        with open(local_metrics_path, 'w') as f:
            json.dump(job_metrics, f, indent=4)
        print(f"Metrics saved locally to {local_metrics_path}")

        # Upload metrics to GCS
        gcs_bucket_name = "<your-bucket-name>"  # Replace with your GCS bucket name
        save_to_gcs(gcs_bucket_name, gcs_metrics_path, local_metrics_path)

    except Exception as e:
        print(f"Error: {e}")
