import json
import base64
import time

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

TOPIC = "projects/{PROJECT_ID}/topics/{TOPIC_NAME}"


def send_pubsub_orders(interval_seconds: int = 3):
    with open("resources/orders.json") as lines:
        try:
            for line in lines:
                orders = json.dumps(line)
                orders_bytes = orders.encode("ascii")
                base64_bytes = base64.b64encode(orders_bytes)

                # Publish base64_bytes messages to PubSub
                send_base64_orders = publisher.publish(TOPIC, base64_bytes)

                print(send_base64_orders.result())  # to watch which message send
                print("Message published successfully")
                print("******************************")
                time.sleep(interval_seconds)  # Add a sleep
        except Exception as e:
            print(f"Error publishing message: {e}")


send_pubsub_orders()