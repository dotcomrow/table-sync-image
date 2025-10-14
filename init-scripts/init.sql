CREATE DATABASE testdb;

\c testdb

CREATE TABLE public.testtable (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.debezium_signal (
  id   text PRIMARY KEY,
  type text NOT NULL,
  data jsonb,
  table_database text
);

COMMENT ON TABLE public.testtable IS
'{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.testtable"}}';

INSERT INTO public.testtable (name) VALUES ('Sample Name 1'), ('Sample Name 2');
INSERT INTO public.testtable (name) VALUES ('Sample Name 3'), ('Sample Name 4');

CREATE SCHEMA IF NOT EXISTS test_schema;

CREATE TABLE test_schema.testtable (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

INSERT INTO test_schema.testtable (name) VALUES ('Sample Name 1'), ('Sample Name 2');
INSERT INTO test_schema.testtable (name) VALUES ('Sample Name 3'), ('Sample Name 4');

COMMENT ON TABLE test_schema.testtable IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.testtable"}}';

CREATE TABLE test_schema.mcp_openapi_usage_hints (
  augmentation_id INT NOT NULL,
  hint VARCHAR(255) NOT NULL
);

COMMENT ON TABLE test_schema.testtable IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.testtable"}}';

COMMENT ON TABLE test_schema.mcp_openapi_usage_hints IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.mcp_openapi_usage_hints"}}';

INSERT INTO test_schema.mcp_openapi_usage_hints (augmentation_id, hint) VALUES (1, 'Hint 1'), (2, 'Hint 2');