from pyspark.sql import SparkSession

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WriteCSVExample") \
    .getOrCreate()

# Create a DataFrame with 5 records
data = [
    (1, "John", "Doe", 28, "Engineer"),
    (2, "Jane", "Smith", 34, "Doctor"),
    (3, "Alice", "Johnson", 29, "Teacher"),
    (4, "Bob", "Brown", 45, "Manager"),
    (5, "Charlie", "Davis", 38, "Artist")
]
columns = ["ID", "FirstName", "LastName", "Age", "Occupation"]

df = spark.createDataFrame(data, columns)

# Write the DataFrame to a CSV file
output_path = "csv_example.csv"  # Update this path as needed
df.write.csv(output_path, header=True, mode="overwrite")

print(f"CSV file written to {output_path}")

# Stop the Spark session
spark.stop()
