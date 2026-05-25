include .local/.env
export

TEST_DB_IMAGE=ghcr.io/godfatherofdevil/postgres-18-alpine-wal2json:latest
TEST_DB_NAME=tests
TEST_DB_USER=tests
TEST_DB_PASSWORD=secret
TEST_DB_PORT=5432
TEST_DB_CONTAINER=pgwal-tests
TEST_DB_SUPERUSER=$(TEST_DB_USER)
TEST_RABBITMQ_IMAGE=rabbitmq:4.3-alpine
TEST_RABBITMQ_PORT=5672
TEST_RABBITMQ_CONTAINER=pgwal-rabbit-tests
TEST_RABBITMQ_HOST=localhost
TEST_RABBITMQ_USER=tests
TEST_RABBITMQ_PASSWORD=secret
TEST_RABBITMQ_VHOST=/

.PHONY: run_rabbitmq_test
run_rabbitmq_test:
	docker run --rm -d \
		-p $(TEST_RABBITMQ_PORT):5672 \
		--name $(TEST_RABBITMQ_CONTAINER) \
		-e RABBITMQ_DEFAULT_USER=$(TEST_RABBITMQ_USER) \
		-e RABBITMQ_DEFAULT_PASS=$(TEST_RABBITMQ_PASSWORD) \
		--health-cmd "rabbitmq-diagnostics -q ping" \
		--health-interval 2s \
		--health-timeout 5s \
		--health-retries 30 \
		$(TEST_RABBITMQ_IMAGE)

.PHONY: wait_rabbitmq_test
wait_rabbitmq_test:
	/bin/bash -c 'for _ in $$(seq 1 60); do \
		status=$$(docker inspect --format="{{.State.Health.Status}}" $(TEST_RABBITMQ_CONTAINER) 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	docker logs $(TEST_RABBITMQ_CONTAINER); \
	exit 1'

.PHONY: run_kafka
run_kafka:
	docker run -d --rm -p 9092:9092 --name kafka-broker apache/kafka:latest

.PHONY: run_brokers
run_brokers: run_rabbitmq_test run_kafka
	docker ps -a

.PHONY: run_psql_test
run_psql_test:
	docker run --rm -d \
		-p $(TEST_DB_PORT):5432 \
		-e POSTGRES_DB=$(TEST_DB_NAME) \
		-e POSTGRES_USER=$(TEST_DB_USER) \
		-e POSTGRES_PASSWORD=$(TEST_DB_PASSWORD) \
		--name $(TEST_DB_CONTAINER) \
		--health-cmd "pg_isready -h 127.0.0.1 -U $(TEST_DB_USER) -d $(TEST_DB_NAME)" \
		--health-interval 2s \
		--health-timeout 5s \
		--health-retries 30 \
		$(TEST_DB_IMAGE)

.PHONY: wait_psql_test
wait_psql_test:
	/bin/bash -c 'for _ in $$(seq 1 60); do \
		status=$$(docker inspect --format="{{.State.Health.Status}}" $(TEST_DB_CONTAINER) 2>/dev/null); \
		if [ "$$status" = "healthy" ]; then \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	docker logs $(TEST_DB_CONTAINER); \
	exit 1'

.PHONY: bootstrap_psql_test
bootstrap_psql_test:
	docker exec $(TEST_DB_CONTAINER) psql \
		-h 127.0.0.1 \
		-U $(TEST_DB_SUPERUSER) \
		-d $(TEST_DB_NAME) \
		-c "ALTER ROLE $(TEST_DB_USER) WITH REPLICATION;"

.PHONY: test
test:
	/bin/bash -c "TEST_RABBITMQ_HOST=$(TEST_RABBITMQ_HOST) TEST_RABBITMQ_PORT=$(TEST_RABBITMQ_PORT) TEST_RABBITMQ_USER=$(TEST_RABBITMQ_USER) TEST_RABBITMQ_PASSWORD=$(TEST_RABBITMQ_PASSWORD) TEST_RABBITMQ_VHOST=$(TEST_RABBITMQ_VHOST) python -m coverage run -m pytest;make clean"

.PHONY: clean
clean:
	/bin/bash -c 'docker stop $(TEST_DB_CONTAINER) --timeout 0 >/dev/null 2>&1 || true'
	/bin/bash -c 'docker stop $(TEST_RABBITMQ_CONTAINER) --timeout 0 >/dev/null 2>&1 || true'

.PHONY: cov_report
cov_report:
	python -m coverage report

.PHONY: run_tests
run_tests: run_psql_test wait_psql_test bootstrap_psql_test run_rabbitmq_test wait_rabbitmq_test
	make test && make cov_report

.PHONY: update_docs_structure
update_docs_structure:
	python scripts/update_project_structure.py
