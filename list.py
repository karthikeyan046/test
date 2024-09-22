from google.cloud import pubsub_v1

# Create a client
subscriber = pubsub_v1.SubscriberClient()

# Define your subscription path
subscription_path = "projects/your-project-id/subscriptions/your-subscription-id"

# Function to handle received messages
def callback(message):
    print(f"Received message: {message.data}")
    message.ack()

# Subscribe to the subscription and listen for messages
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive
try:
    streaming_pull_future.result()
except KeyboardInterrupt:
    streaming_pull_future.cancel()
