   from pyspark.sql import SparkSession
   from sparkmeasure import StageMetrics

   #   pip install sparkmeasure
   spark = SparkSession.builder.getOrCreate()

   # Create a StageMetrics object
   stage_metrics = StageMetrics(spark)

   # Start collecting metrics
   stage_metrics.begin()

   # Your PySpark code here
   df = spark.read.csv("your_data.csv")
   df.show()

   # Stop collecting metrics
   stage_metrics.end()

   # Get metrics as a dictionary
   metrics = stage_metrics.aggregate_stage_metrics()

   # Convert metrics to PySpark DataFrame
   metrics_df = spark.createDataFrame(metrics.items(), ["metric_name", "metric_value"])

   # Show the metrics DataFrame
   metrics_df.show()
