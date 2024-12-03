Instead of `from pyspark.scheduler import SparkListener, SparkListenerEvent`, try:
```
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SparkListener").getOrCreate()
from pyspark.scheduler import SparkListener, SparkListenerEven