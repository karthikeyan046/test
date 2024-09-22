
from google.cloud import pubsub

# Create Pub/Sub client
pubsub_client = pubsub.PublisherClient()
subscription_client = pubsub.SubscriberClient()

# Define project ID
project_id = 'my-project'

# Define topic name
topic_name = 'my-topic'

# Define subscription name
subscription_name = 'my-subscription'

# Create topic
topic_path = pubsub_client.topic_path(project_id, topic_name)
topic = pubsub_client.create_topic(request={'name': topic_path})

print(f'Topic created: {topic.name}')

# Create subscription
subscription_path = subscription_client.subscription_path(project_id, subscription_name)
subscription = subscription_client.create_subscription(
    request={'name': subscription_path, 'topic': topic_path}
)

print(f'Subscription created: {subscription.name}')

# Publish message to topic
message_data = 'Hello, world!'.encode('utf-8')
pubsub_client.publish(topic_path, message_data)

print(f'Message published to {topic_name}')

# Pull message from subscription
response = subscription_client.pull(
    request={'subscription': subscription_path, 'max_messages': 1}
)

for message in response.received_messages:
    print(f'Received message: {message.message.data.decode("utf-8")}')
    # Acknowledge message
    subscription_client.acknowledge(
        request={'subscription': subscription_path, 'ack_ids': [message.ack_id]}
   )
