from pyspark.sql import SparkSession
import os

def read_and_transform_pyspark(file_path, output_path):
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Dataproc File Reader and Transformer") \
        .getOrCreate()

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
    df = df.withColumn("processed", spark.sql.functions.lit("yes"))
    
    # Write the transformed data to a Parquet file
    df.write.mode("overwrite").parquet(output_path)
    print(f"Processed file saved to: {output_path}")
    return output_path

if __name__ == "__main__":
    # Replace these paths with GCS paths for Dataproc
    input_file_path = "gs://your-bucket-name/input/example.csv"  # Input file path
    output_file_path = "gs://your-bucket-name/output/example_processed.parquet"  # Output file path

    try:
        processed_file = read_and_transform_pyspark(input_file_path, output_file_path)
        print(f"File processed successfully: {processed_file}")
    except ValueError as e:
        print(f"Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
