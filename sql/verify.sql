-- Ferryman phase 0 exit check. Run against source with -v expect_seed=1, target with 0.
--   psql "$SOURCE_DSN" -v expect_seed=1 -f sql/verify.sql
-- Fails loudly (assertion error, non-zero exit) if any phase 0 guarantee is missing.

\if :{?expect_seed}
\else
\set expect_seed 0
\endif

-- psql does not substitute variables inside dollar-quoted bodies, so hand the
-- value to the DO block through a session GUC instead.
SET ferryman.expect_seed = :expect_seed;

DO $$
DECLARE
    expect_seed int := current_setting('ferryman.expect_seed')::int;
    toast_rel   text;
    n           bigint;
BEGIN
    ASSERT current_setting('wal_level') = 'logical',
        'wal_level is ' || current_setting('wal_level') || ', need logical';
    ASSERT current_setting('max_replication_slots')::int >= 8, 'max_replication_slots < 8';
    ASSERT current_setting('max_wal_senders')::int >= 8, 'max_wal_senders < 8';

    -- 'f' = REPLICA IDENTITY FULL. Anything else loses old-row data on UPDATE/DELETE.
    FOR n IN SELECT 1 FROM pg_class
             WHERE relname IN ('users','org_members','audit_log')
               AND relkind = 'r' AND relreplident <> 'f'
    LOOP
        RAISE EXCEPTION 'a table is not REPLICA IDENTITY FULL';
    END LOOP;

    SELECT count(*) INTO n FROM pg_class
     WHERE relname IN ('users','org_members','audit_log') AND relkind = 'r';
    ASSERT n = 3, 'expected 3 tables, found ' || n;

    IF expect_seed = 1 THEN
        SELECT count(*) INTO n FROM users;
        ASSERT n >= 100000, 'users has ' || n || ' rows, expected >= 100000';

        SELECT count(*) INTO n FROM org_members;
        ASSERT n >= 150000, 'org_members has ' || n || ' rows, expected >= 150000';

        SELECT count(*) INTO n FROM audit_log;
        ASSERT n >= 50000, 'audit_log has ' || n || ' rows, expected >= 50000';

        -- Prove profile really went out-of-line: the TOAST relation must hold chunks.
        -- pg_column_size() alone would not distinguish inline from TOASTed storage.
        SELECT t.relname INTO toast_rel
          FROM pg_class c JOIN pg_class t ON t.oid = c.reltoastrelid
         WHERE c.relname = 'users';
        ASSERT toast_rel IS NOT NULL, 'users has no TOAST relation';
        EXECUTE format('SELECT count(*) FROM pg_toast.%I', toast_rel) INTO n;
        ASSERT n > 0, 'no TOASTed values in users.profile; seed payload too small';
    ELSE
        SELECT count(*) INTO n FROM users;
        ASSERT n = 0, 'target should start empty, found ' || n || ' users';
    END IF;

    RAISE NOTICE 'phase 0 verify OK (expect_seed=%)', expect_seed;
END $$;
