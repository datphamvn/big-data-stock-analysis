import sys, socket
from pathlib import Path
path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))

from confluent_kafka import Consumer, KafkaError
from script.utils import load_environment_variables
from dotenv import load_dotenv

load_dotenv()
env_vars = load_environment_variables()
# Configuration for Kafka Consumer
conf = {
    # Pointing to brokers. Ensure these match the host and ports of your Kafka brokers.
    'bootstrap.servers': env_vars.get("KAFKA_BROKERS"),
    'group.id': "myGroup",  # Consumer group ID. Change as per your requirement.
    'auto.offset.reset': 'earliest'  # Start from the earliest messages if no offset is stored.
}
consumer = Consumer(conf)
# Subscribe to the topic
consumer.subscribe([env_vars.get("STOCK_PRICE_KAFKA_TOPIC"),])

try:
    # Continuously poll for new messages
    while True:
        msg = consumer.poll(timeout=1.0)  # Poll for a message

        # Check for message. If none, continue polling
        if msg is None:
            continue

        # Check for errors
        if msg.error():
            # Continue if it's an end of partition event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                # Print any other error and break from the loop
                print(msg.error())
                break

        # Decode and print the message received
        print('Received message: {}'.format(msg.value().decode('utf-8')))

# Allow for graceful shutdown on interrupt
except KeyboardInterrupt:
    pass

finally:
    # Close down the consumer to commit final offsets.
    consumer.close()
