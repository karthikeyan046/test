from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.status import JobListener

class CustomSparkListener(JobListener):
    def __init__(self):
        self.job_metrics = []

    def onJobStart(self, job_id):
        print(f"Job {job_id} started.")

    def onJobEnd(self, job_id):
        print(f"Job {job_id} ended.")
        self.job_metrics.append({"jobId": job_id})

    def get_metrics(self):
        return self.job_metrics
