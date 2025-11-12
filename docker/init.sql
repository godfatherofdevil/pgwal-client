-- create test database
CREATE DATABASE tests;
-- create test user
CREATE USER tests WITH PASSWORD 'secret';
-- give all privileges to this test user
ALTER USER tests WITH SUPERUSER;
