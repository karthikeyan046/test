from google.cloud import dataproc_v1
from google.cloud.dataproc_v1.gapic.transports import JobControllerGrpcTransport

# Initialize the Dataproc job controller client
client = dataproc_v1.JobControllerClient(
    transport=JobControllerGrpcTransport(channel="grpc://dataproc.googleapis.com")
)

# Set your cluster name and region
project_id = 'your-project-id'
region = 'us-central1'
cluster_name = 'your-cluster-name'

# Define the PySpark job
job = {
    'placement': {
        'cluster_name': cluster_name
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://your-bucket/scripts/my_pyspark_script.py'
    }
}

# Submit the job to the cluster
operation = client.submit_job(project_id=project_id, region=region, job=job)

# Wait for the job to complete and fetch the result
response = operation.result()

print("Job finished with status: ", response.status.state)
