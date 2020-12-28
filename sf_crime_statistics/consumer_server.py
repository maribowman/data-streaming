from kafka import KafkaConsumer


def listen():
    consumer = KafkaConsumer("sf.police.department.calls",
                             bootstrap_servers=["localhost:9092"],
                             client_id="sf-crime-consumer"
                             )
    for message in consumer:
        print(f"{message.topic}:{message.offset}:\nkey={message.key} value={message.value}")


if __name__ == "__main__":
    listen()
