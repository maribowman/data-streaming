compose:
	@docker-compose -f ./docker-compose.yaml start

compose-stop:
	@docker-compose -f ./docker-compose.yaml stop

start-producer:
	@python3 kafka_server.py

consumer-server:
	@python3 consumer_server.py

consumer-console:
	@kafkacat -C -b localhost:9092 -t sf.police.department.calls