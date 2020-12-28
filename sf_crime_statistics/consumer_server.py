from kafka import KafkaConsumer


def listen():
    consumer = KafkaConsumer("sf.police.department.calls",
                             bootstrap_servers=["localhost:9092"],
                             client_id="sf-crime-consumer"
                             )
    # print(f"subscribed to: {consumer.subscription()}")
    for message in consumer:
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print(f"{message.topic}:{message.offset}:\nkey={message.key} value={message.value}")


if __name__ == "__main__":
    listen()
