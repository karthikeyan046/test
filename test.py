from google.cloud import pubsub_v1

# Create a client
client = pubsub_v1.PublisherClient()

# Your GCP project ID
project_id = "your-project-id"
project_path = f"projects/{project_id}"

# List all topics
for topic in client.list_topics(request={"project": project_path}):
    print(topic.name)
