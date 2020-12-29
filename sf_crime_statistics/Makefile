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

spark:
	@spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7 --master local[*] data_stream.py > spark.log