
from google.cloud import pubsub

# Create clients
pubsub_client = pubsub.PublisherClient()
subscription_client = pubsub.SubscriberClient()

# Define project, topic, and subscription names
project_id = 'my-project'
topic_name = 'my-topic'
subscription_name = 'my-subscription'

# Define topic and subscription paths
topic_path = pubsub_client.topic_path(project_id, topic_name)
subscription_path = subscription_client.subscription_path(project_id, subscription_name)

# Create subscription if it doesn't exist
subscription = subscription_client.create_subscription(
    request={'name': subscription_path, 'topic': topic_path}
)

# Pull messages
response = subscription_client.pull(
    request={'subscription': subscription_path, 'max_messages': 10}
)

# Print messages
for message in response.received_messages:
    print(message.message.data.decode('utf-8'))
    # Acknowledge message
    subscription_client.acknowledge(
        request={'subscription': subscription_path, 'ack_ids': [message.ack_id]}
)
