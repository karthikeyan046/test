import json
import random
import time
from google.cloud import pubsub_v1
from datetime import datetime, timezone

# Initialize Pub/Sub publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path('your-project-id', 'banking-transactions-topic')

# Function to generate random banking transaction
def generate_transaction():
    transaction_id = f"TX{random.randint(10000, 99999)}"
    customer_id = f"CUST{random.randint(1000, 9999)}"
    transaction_amount = round(random.uniform(10.0, 1000.0), 2)
    transaction_type = random.choice(['Credit', 'Debit', 'Transfer'])
    timestamp = datetime.now(timezone.utc).isoformat()

    return {
        "TransactionID": transaction_id,
        "CustomerID": customer_id,
        "TransactionAmount": transaction_amount,
        "TransactionType": transaction_type,
        "Timestamp": timestamp
    }

# Loop to publish transactions every 5 seconds
try:
    while True:
        # Generate a random transaction
        transaction = generate_transaction()

        # Publish the transaction to Pub/Sub
        publisher.publish(topic_path, data=json.dumps(transaction).encode('utf-8'))
        
        print(f"Published transaction: {transaction}")

        # Sleep for 5 seconds before publishing the next one
        time.sleep(5)

except KeyboardInterrupt:
    print("Stopped transaction publishing.")
