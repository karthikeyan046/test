from google.cloud import pubsub_v1

# Create a client
publisher = pubsub_v1.PublisherClient()

# Define the topic path
project_id = "your-project-id"
topic_name = "my-topic"
topic_path = publisher.topic_path(project_id, topic_name)

# Create the topic
topic = publisher.create_topic(request={"name": topic_path})
print(f"Topic created: {topic.name}")
