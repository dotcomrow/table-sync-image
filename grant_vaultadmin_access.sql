-- SQL commands to grant vaultadmin comprehensive access to existing kafka database
-- Run these commands as a superuser (like yugabyte or postgres)

-- 1. Grant all privileges on the kafka database
GRANT ALL PRIVILEGES ON DATABASE kafka TO vaultadmin;

-- 2. Connect to the kafka database first
\c kafka

-- 3. Make vaultadmin owner of the public schema
ALTER SCHEMA public OWNER TO vaultadmin;

-- 4. Grant all privileges on the public schema
GRANT ALL ON SCHEMA public TO vaultadmin;

-- 5. Grant all privileges on existing objects in public schema
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO vaultadmin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO vaultadmin;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO vaultadmin;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA public TO vaultadmin;

-- 6. Set default privileges for future objects created by any user
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO vaultadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO vaultadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO vaultadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON PROCEDURES TO vaultadmin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TYPES TO vaultadmin;

-- 7. Set default privileges for objects created by vaultadmin
ALTER DEFAULT PRIVILEGES FOR USER vaultadmin IN SCHEMA public GRANT ALL ON TABLES TO vaultadmin;
ALTER DEFAULT PRIVILEGES FOR USER vaultadmin IN SCHEMA public GRANT ALL ON SEQUENCES TO vaultadmin;
ALTER DEFAULT PRIVILEGES FOR USER vaultadmin IN SCHEMA public GRANT ALL ON FUNCTIONS TO vaultadmin;
ALTER DEFAULT PRIVILEGES FOR USER vaultadmin IN SCHEMA public GRANT ALL ON PROCEDURES TO vaultadmin;
ALTER DEFAULT PRIVILEGES FOR USER vaultadmin IN SCHEMA public GRANT ALL ON TYPES TO vaultadmin;

-- 8. Grant CREATE privilege on database for creating new schemas
GRANT CREATE ON DATABASE kafka TO vaultadmin;

-- 9. Verify the grants (optional - shows what was granted)
SELECT 
    datname as database_name,
    datacl as database_acl
FROM pg_database 
WHERE datname = 'kafka';

SELECT 
    schemaname,
    schemaowner,
    schemaacl
FROM pg_stat_user_tables 
WHERE schemaname = 'public'
LIMIT 1;