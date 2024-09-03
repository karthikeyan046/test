from google.cloud import pubsub_v1
#pip install google-cloud-pubsub

def publish_message(project_id, topic_id, message):
    # Create a Publisher client
    publisher = pubsub_v1.PublisherClient()
    # Create a fully qualified identifier in the form of `projects/{project_id}/topics/{topic_id}`
    topic_path = publisher.topic_path(project_id, topic_id)
    
    # Data must be a bytestring
    data = message.encode("utf-8")
    
    # Publish a message to the topic
    future = publisher.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

if __name__ == "__main__":
    project_id = "your-project-id"  # Replace with your GCP project ID
    topic_id = "your-topic-id"      # Replace with your Pub/Sub topic ID
    message = "Hello, Pub/Sub!"     # Replace with your message content
    
    publish_message(project_id, topic_id, message)
