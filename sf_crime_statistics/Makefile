kafka-start:
	@docker-compose -f ./docker-compose.yaml start

kafka-stop:
	@docker-compose -f ./docker-compose.yaml stop

start:
	@python3 kafka_server.py

consumer:
	@kafkacat -C -b localhost:9092 -t sf.police.department.calls