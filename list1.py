Here's a Python example using the Google Cloud Pub/Sub Client Library to read messages from a topic:


```
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
```

Make sure to:


1. Install the Google Cloud Pub/Sub Client Library: `pip install google-cloud-pubsub`
2. Set up authentication: `gcloud auth login` or `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. Replace `my-project`, `my-topic`, and `my-subscription` with your actual values.


For more information, refer to:


- Google Cloud Pub/Sub Documentation: (link unavailable)
- Google Cloud Pub/Sub Client Library: (link unavailable)