FROM ghcr.io/godfatherofdevil/postgres-18-alpine-wal2json:latest
COPY docker/init.sql /docker-entrypoint-initdb.d/init.sql

CMD ["postgres", "-c", "config_file=/var/lib/postgresql/data/postgresql.conf"]
