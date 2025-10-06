CREATE DATABASE testdb;

\c testdb

CREATE TABLE public.testtable (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE public.testtable IS
'{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.testtable"}}';