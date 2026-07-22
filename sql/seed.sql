-- Ferryman phase 0: source-only seed. 100k users + 150k org_members + 50k audit rows.
-- ponytail: generate_series does the whole job, no Go seeder to maintain.

INSERT INTO users (email, profile)
SELECT
    'user' || i || '@example.com',
    jsonb_build_object(
        'id', i,
        'tier', (ARRAY['free','pro','enterprise'])[1 + i % 3],
        -- ~4KB of payload forces the row over TOAST_TUPLE_THRESHOLD so profile
        -- is stored out-of-line. Phase 2 relies on these being real TOAST values.
        'blob', repeat(md5(i::text), 128)
    )
FROM generate_series(1, 100000) AS i;

INSERT INTO org_members (org_id, user_id, role)
SELECT
    1 + i % 500,
    i,
    (ARRAY['member','admin','owner'])[1 + i % 3]
FROM generate_series(1, 100000) AS i
ON CONFLICT DO NOTHING;

-- second membership per user in a different org, to make the composite key matter
INSERT INTO org_members (org_id, user_id, role)
SELECT 501 + i % 500, i, 'member'
FROM generate_series(1, 50000) AS i
ON CONFLICT DO NOTHING;

INSERT INTO audit_log (actor_id, action)
SELECT 1 + i % 100000, 'seed.create'
FROM generate_series(1, 50000) AS i;

ANALYZE;
