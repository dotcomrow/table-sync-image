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
  data jsonb
);

COMMENT ON TABLE public.testtable IS
'{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.testtable"}}';

INSERT INTO public.testtable (name) VALUES ('Sample Name 1'), ('Sample Name 2');
INSERT INTO public.testtable (name) VALUES ('Sample Name 3'), ('Sample Name 4');