.PHONY: run_rabbitmq
run_rabbitmq:
	docker run -d --rm -p 5672:5672 -p 15672:15672 --name rabbit-broker rabbitmq:3.13-management-alpine

.PHONY: run_kafka
run_kafka:
	docker run -d --rm -p 9092:9092 --name kafka-broker apache/kafka:latest

.PHONY: run_brokers
run_brokers: run_rabbitmq run_kafka
	docker ps -a

.PHONY: run_tests
run_tests:
	python -m coverage run -m pytest
