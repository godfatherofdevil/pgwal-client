include .local/.env
export

.PHONY: login_github_cr
login_github_cr:
	echo $(GITHUB_CR_TOKEN) | docker login ghcr.io -u $(GITHUB_CR_USERNAME) --password-stdin

.PHONY: run_rabbitmq
run_rabbitmq:
	docker run -d --rm -p 5672:5672 -p 15672:15672 --name rabbit-broker rabbitmq:3.13-management-alpine

.PHONY: run_kafka
run_kafka:
	docker run -d --rm -p 9092:9092 --name kafka-broker apache/kafka:latest

.PHONY: run_brokers
run_brokers: run_rabbitmq run_kafka
	docker ps -a

.PHONY: build_psql_test
build_psql_test_local: login_github_cr
	docker build . -t psql-18-test -f docker/psql.test.Dockerfile

.PHONY: run_psql_test
run_psql_test:
	docker run --rm -d -p 5432:5432 -e POSTGRES_PASSWORD=secret --name pgwal-tests psql-18-test

.PHONY: test
test:
	/bin/bash -c "python -m coverage run -m pytest;make clean"

.PHONY: clean
clean:
	docker stop pgwal-tests --time 0

.PHONY: cov_report
cov_report:
	python -m coverage report

.PHONY: run_tests
run_tests: run_psql_test
	echo "Running Tests. Waiting for 1 second for the db instance to be update" && sleep 1
	make test && make cov_report
