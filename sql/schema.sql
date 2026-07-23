-- Ferryman phase 0: dual-schema base. Applied identically to source_db and target_db.
-- Shapes chosen to exercise every WAL decoding hazard in CLAUDE.md:
--   users        -> single-column PK + TOASTed jsonb + sequence to sync at cutover
--   org_members  -> composite PK (org_id, user_id)
--   audit_log    -> no PK at all, only survivable with REPLICA IDENTITY FULL

CREATE TABLE IF NOT EXISTS users (
    id         bigserial PRIMARY KEY,
    email      text NOT NULL,
    -- profile is deliberately large so postgres pushes it out-of-line into TOAST
    profile    jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS org_members (
    org_id  bigint NOT NULL,
    user_id bigint NOT NULL,
    role    text   NOT NULL,
    PRIMARY KEY (org_id, user_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    actor_id bigint      NOT NULL,
    action   text        NOT NULL,
    at       timestamptz NOT NULL DEFAULT now()
);

-- EXTENDED (the default) compresses before it TOASTs, and our seed payload is
-- repetitive enough to compress back under the 2KB threshold — which would leave
-- nothing out-of-line to test against. EXTERNAL skips compression so large
-- profiles are always TOASTed, which is what phase 2's unchanged-TOAST flag needs.
ALTER TABLE users ALTER COLUMN profile SET STORAGE EXTERNAL;

-- Without this, UPDATE/DELETE frames carry only the PK (and nothing at all for
-- audit_log), which silently diverges the target. Danger zone #1.
ALTER TABLE users       REPLICA IDENTITY FULL;
ALTER TABLE org_members REPLICA IDENTITY FULL;
ALTER TABLE audit_log   REPLICA IDENTITY FULL;
