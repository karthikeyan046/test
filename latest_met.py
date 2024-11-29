import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from google.cloud import storage


def save_to_gcs(bucket_name, destination_blob_name, local_file_path):
    """
    Uploads a local file to a Google Cloud Storage bucket.

    Args:
        bucket_name (str): The GCS bucket name.
        destination_blob_name (str): The destination path in the GCS bucket.
        local_file_path (str): The local file path to upload.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)
    print(f"File {local_file_path} uploaded to gs://{bucket_name}/{destination_blob_name}")


def read_and_transform_pyspark(file_path, output_path, spark):
    """
    Reads a file using PySpark, transforms it, and writes the output to Parquet.

    Args:
        file_path (str): Input file path in GCS.
        output_path (str): Output file path in GCS.
        spark (SparkSession): Active Spark session.
    """
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn("processed", lit("yes"))
    df.write.mode("overwrite").parquet(output_path)
    print(f"Processed file saved to: {output_path}")


def monitor_job_status(spark_context, interval=5):
    """
    Monitor Spark job and stage progress using statusTracker.
    
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
        .appName("GCS Metrics Logging") \
        .getOrCreate()

    # File paths
    input_file_path = "gs://<your-bucket-name>/example.csv"  # Replace with your GCS input path
    output_file_path = "gs://<your-bucket-name>/example_processed.parquet"  # Replace with your GCS output path
    local_metrics_path = "job_metrics.json"  # Local temporary file for metrics
    gcs_metrics_path = "metrics/job_metrics.json"  # GCS destination path for metrics

    try:
        # Read and process the file
        read_and_transform_pyspark(input_file_path, output_file_path, spark)

        # Monitor and collect job metrics
        metrics = monitor_job_status(spark.sparkContext)

        # Save metrics locally
        with open(local_metrics_path, 'w') as f:
            json.dump(metrics, f, indent=4)
        print(f"Metrics saved locally to {local_metrics_path}")

        # Upload metrics to GCS
        gcs_bucket_name = "<your-bucket-name>"  # Replace with your GCS bucket name
        save_to_gcs(gcs_bucket_name, gcs_metrics_path, local_metrics_path)

    except Exception as e:
        print(f"Error: {e}")
