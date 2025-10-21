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

CREATE TABLE IF NOT EXISTS public.database_stream (
  stream_id text PRIMARY KEY
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

CREATE TABLE test_schema.mcp_openapi_augmentations (
  id            SERIAL PRIMARY KEY,
  path          TEXT NOT NULL,
  method        TEXT NOT NULL,
  summary       TEXT,
  description   TEXT,
  auth_required BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE test_schema.mcp_openapi_usage_hints (
  augmentation_id INTEGER NOT NULL REFERENCES test_schema.mcp_openapi_augmentations(id) ON DELETE CASCADE,
  hint            TEXT NOT NULL,
  id              SERIAL PRIMARY KEY,
  -- prevent duplicate hints per augmentation (optional but recommended)
  CONSTRAINT uq_usage_hints_aug_hint UNIQUE (augmentation_id, hint)
);

CREATE TABLE test_schema.mcp_openapi_param_hints (
  augmentation_id INTEGER NOT NULL REFERENCES test_schema.mcp_openapi_augmentations(id) ON DELETE CASCADE,
  name            TEXT NOT NULL,
  data_type       TEXT,
  allowed_values  TEXT[],   -- or a separate table if you want more structure
  example_value   TEXT,
  default_value   TEXT,
  description     TEXT,
  id              SERIAL PRIMARY KEY,
  -- enable ON CONFLICT (augmentation_id, name)
  CONSTRAINT uq_param_hints_aug_name UNIQUE (augmentation_id, name)
);

CREATE TABLE test_schema.mcp_openapi_examples (
  augmentation_id INTEGER NOT NULL REFERENCES test_schema.mcp_openapi_augmentations(id) ON DELETE CASCADE,
  example_index   INTEGER NOT NULL, -- allows multiple examples per augmentation
  user_prompt     TEXT,
  args_json       JSONB NOT NULL,
  id              SERIAL PRIMARY KEY,
  -- enable ON CONFLICT (augmentation_id, example_index)
  CONSTRAINT uq_examples_aug_idx UNIQUE (augmentation_id, example_index)
);

-- (Optional) helpful indexes (FK columns are often queried)
CREATE INDEX IF NOT EXISTS idx_usage_hints_aug_id  ON test_schema.mcp_openapi_usage_hints(augmentation_id);
CREATE INDEX IF NOT EXISTS idx_param_hints_aug_id  ON test_schema.mcp_openapi_param_hints(augmentation_id);
CREATE INDEX IF NOT EXISTS idx_examples_aug_id     ON test_schema.mcp_openapi_examples(augmentation_id);

COMMENT ON TABLE test_schema.mcp_openapi_augmentations IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.mcp_openapi_augmentations"}}';

COMMENT ON TABLE test_schema.mcp_openapi_examples IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.mcp_openapi_examples"}}';

COMMENT ON TABLE test_schema.mcp_openapi_param_hints IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.mcp_openapi_param_hints"}}';

COMMENT ON TABLE test_schema.mcp_openapi_usage_hints IS
'{"bootstrap":{"enabled":true, "bq": "test_schema.mcp_openapi_usage_hints"}}';
