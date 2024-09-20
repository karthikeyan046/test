from google.cloud import pubsub_v1
from google.cloud import bigquery
import json

# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path('your-project-id', 'banking-transactions-subscription')

# Initialize BigQuery client
bq_client = bigquery.Client()
dataset_id = 'banking_dataset'
table_id = 'transactions'

def callback(message):
    print(f"Received message: {message.data}")
    transaction = json.loads(message.data)

    # Insert into BigQuery
    rows_to_insert = [
        {
            u'TransactionID': transaction['TransactionID'],
            u'CustomerID': transaction['CustomerID'],
            u'TransactionAmount': transaction['TransactionAmount'],
            u'TransactionType': transaction['TransactionType'],
            u'Timestamp': transaction['Timestamp'],
        }
    ]

    errors = bq_client.insert_rows_json(f'{dataset_id}.{table_id}', rows_to_insert)
    if errors == []:
        print(f"New row has been added to BigQuery: {transaction}")
        message.ack()  # Acknowledge message
    else:
        print(f"Failed to insert rows into BigQuery: {errors}")

subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

# Keep the main thread alive to continue listening for messages
import time
while True:
    time.sleep(60)
