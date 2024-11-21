import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.scheduler import SparkListener, SparkListenerJobStart, SparkListenerJobEnd
import time


class CustomSparkListener(SparkListener):
    """
    Custom Spark Listener to capture job metrics.
    """

    def __init__(self):
        super(CustomSparkListener, self).__init__()
        self.job_metrics = []

    def onJobStart(self, job_start: SparkListenerJobStart):
        print(f"Job {job_start.jobId} started at {time.time()} with {len(job_start.stageInfos)} stages.")

    def onJobEnd(self, job_end: SparkListenerJobEnd):
        print(f"Job {job_end.jobId} ended with status: {job_end.jobResult}")
        self.job_metrics.append(
            {
                "jobId": job_end.jobId,
                "result": str(job_end.jobResult),
                "completionTime": time.time()
            }
        )

    def get_metrics(self):
        return self.job_metrics

    def write_metrics_to_file(self, file_path):
        """
        Writes the collected metrics to a JSON file.

        Args:
            file_path (str): The file path to save metrics (GCS path).
        """
        with open(file_path, 'w') as f:
            json.dump(self.job_metrics, f, indent=4)
        print(f"Metrics written to file: {file_path}")


def read_and_transform_pyspark(file_path, output_path, spark):
    """
    Reads a file using PySpark based on its extension, applies a sample transformation,
    and writes the output to a Parquet file.

    Args:
        file_path (str): The path to the file to be processed (GCS path).
        output_path (str): The path to write the processed Parquet file (GCS path).
        spark (SparkSession): The active Spark session.

    Returns:
        str: The path to the transformed Parquet file.
    """
    # Get file extension
    file_extension = os.path.splitext(file_path)[1].lower()

    # Read the file
    if file_extension == '.csv':
        df = spark.read.csv(file_path, header=True, inferSchema=True)
    elif file_extension == '.json':
        df = spark.read.json(file_path)
    elif file_extension == '.parquet':
        df = spark.read.parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_extension}")

    # Sample transformation: Add a new column "processed" with a static value
    df = df.withColumn("processed", lit("yes"))

    # Write the transformed data to a Parquet file
    df.write.mode("overwrite").parquet(output_path)
    print(f"Processed file saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Dataproc File Reader and Transformer with Metrics") \
        .getOrCreate()

    # Attach Custom Listener
    listener = CustomSparkListener()
    spark.sparkContext._jsc.sc().addSparkListener(listener)

    # Replace these paths with your input and output GCS paths
    input_file_path = "gs://<your-bucket-name>/example.csv"  # Replace with your GCS input file path
    output_file_path = "gs://<your-bucket-name>/example_processed.parquet"  # Replace with your GCS output path

    try:
        processed_file = read_and_transform_pyspark(input_file_path, output_file_path, spark)
        print(f"File processed successfully: {processed_file}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    # Save metrics to a JSON file
    metrics_file_path = "gs://<your-bucket-name>/job_metrics.json"  # Replace with your desired GCS metrics file path
    listener.write_metrics_to_file(metrics_file_path)
