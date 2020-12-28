import producer_server


def run_kafka_server(client_id, servers, topic):
    # with codecs.open('./police-department-calls-for-service.json', 'r', 'utf-8') as json_file:
    #     input_file = json.loads(json_file.read())
    # print(json.dumps(input_file, indent=4))

    return producer_server.ProducerServer(
        # zipped data due to github upload limitation of 100mb
        input_file='./police-department-calls-for-service',
        topic=topic,
        bootstrap_servers=servers,
        client_id=client_id
    )


def feed():
    producer = run_kafka_server(client_id="sf-crime-producer", servers="localhost:9092", topic="sf.police.department.calls")
    producer.generate_data()


if __name__ == "__main__":
    feed()
