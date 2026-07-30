# Ferryman

Zero-downtime Postgres-to-Postgres cutover orchestrator.

Ferryman moves a live database onto another Postgres instance without taking the
application offline. It bulk-copies from a replication slot's exported snapshot,
streams the remaining changes over logical replication (`pgoutput`), proves the
two sides match, then switches traffic across a proxy it controls — and keeps a
reverse pipeline running afterwards so the switch can be undone.

It is built for **moving** a database: to a bigger instance, a new region, a new
major version, new hardware. It is **not** a schema-transformation tool — see
[Known limits](#known-limits).

## Quick start

```sh
docker compose up -d --wait          # two postgres 16 instances, seeded
make migrate                         # backfill, stream, verify, cut over
```

`make migrate` runs the pipeline against the compose instances and stops at the
rollback window, where it waits: press <kbd>Enter</kbd> to roll back to the
source, or leave it and the window closes on its own.

Point your application at the router (`127.0.0.1:6432` by default) rather than at
the database, or the cutover has no traffic to switch.

From another shell, while a migration is running:

```sh
make status                          # slot lag, without joining the migration
```

## How a migration runs

Each step depends on the one before it, and `cmd/ferryman/main.go` enforces that
ordering in code rather than in comments — the three constraints marked ⚠ are the
ones that corrupt data silently if they are got wrong.

| Step | What happens |
| --- | --- |
| 1 | Publication and replication slot created on the source. The slot exports a snapshot pinned to its consistent LSN. |
| 2 | ⚠ Backfill copies every table from that snapshot, in parallel `ctid` page ranges. The session that created the slot stays open throughout, because Postgres discards an exported snapshot when its exporting session ends. |
| 3 | ⚠ Only once the backfill returns does CDC begin applying. An `UPDATE` for a row not yet copied would apply to nothing and then be overwritten by the older snapshot row. |
| 4 | Changes stream in commit order and are applied idempotently — upserts, unchanged-TOAST columns omitted, composite and absent primary keys handled. |
| 5 | ⚠ The reverse slot is created on the target *before* cutover. After it, the target's first writes would already have happened with nothing holding their WAL, and a rollback would lose them. |
| 6 | Ferryman waits for lag to fall below `-max-lag` **before** pausing anything. |
| 7 | Cutover: pause traffic, drain open connections, emit a logical-decoding marker and wait for the applier to pass it (zero lag by construction), hash-verify both sides, sync sequences, repoint the router, resume. |
| 8 | Rollback window: writes on the target stream back to the source, and the source is set read-only so a stray writer is refused rather than reconciled. |
| 9 | Window closes — the reverse slot is dropped and the target is permanently authoritative. |

Every failure path in step 7 resumes traffic before returning, against the
source, which is still authoritative because nothing was switched. A failed
cutover is a latency spike, not an outage.

## Commands

```
ferryman migrate   backfill, stream, verify, then switch traffic to the target
ferryman status    report replication lag for a slot, from outside a migration
```

### `migrate` flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `-source` | `$FERRYMAN_SOURCE_DSN` | Source DSN. Required. |
| `-target` | `$FERRYMAN_TARGET_DSN` | Target DSN. Required. |
| `-listen` | `127.0.0.1:6432` | Address clients connect to instead of the database. |
| `-slot` | `ferryman_slot` | Replication slot name. Reusing an existing slot resumes the stream and skips the backfill. |
| `-publication` | `ferryman_pub` | Publication name, created `FOR ALL TABLES`. |
| `-parallelism` | `4` | Backfill workers sharing the exported snapshot. |
| `-max-lag` | `8388608` (8 MiB) | Lag, in bytes, that must be reached before traffic is paused. |
| `-drain-timeout` | `30s` | Budget for the paused part of the cutover. Exceeding it aborts and resumes on the source. |
| `-window` | `1h` | How long the reverse pipeline stays up after cutover. |
| `-verify` | `true` | Hash-compare every table while traffic is paused; abort the cutover if they differ. |
| `-no-cutover` | `false` | Backfill and stream only, printing lag. Never switches traffic. |

## Known limits

Read this section before trusting it with anything.

- **Schemas must match; there is no transformation layer.** Column names and
  types are carried through unchanged — no renames, no type conversions, no
  table splits or merges, no computed defaults. `SyncSchema` reconciles the
  target *towards* the source (adding columns that appeared mid-stream, relaxing
  `NOT NULL` on columns the source dropped); it does not map one schema onto a
  different one. If you need `ADD COLUMN ... NOT NULL DEFAULT ...` with a
  backfill, do it on the source as an ordinary migration and let Ferryman carry
  it across.
- **The drain is connection-level, not transaction-level.** The router proxies
  TCP without parsing the Postgres wire protocol, so "in flight" means an open
  connection, not an open transaction. Application pools hold connections open
  for minutes by default — pgx, HikariCP and SQLAlchemy all do — which will stall
  the pause until `-drain-timeout` and then abort the cutover (safely, on the
  source). Set your pool's maximum connection lifetime below `-drain-timeout`
  before cutting over. Parsing the protocol to track transaction boundaries is
  the upgrade path.
- **Clients must be able to reconnect.** The guarantee is that no connection is
  ever *refused*: while paused, new connections are accepted and held, so clients
  see latency rather than errors. Sessions still open at the switch are not
  migrated across — they end and must be re-established. Clients need retry
  logic.
- **New tables are not replicated, only new columns.** A table created on the
  source mid-migration makes the applier stop with an instruction to create it on
  the target first. A faithful `CREATE TABLE` means indexes, constraints,
  identity columns and their sequences, and guessing at those is worse than
  stopping.
- **Changes are applied per row, not per transaction.** Each row change is its
  own autocommit statement, so during sync the target does not observe source
  transactions atomically, and throughput is one round trip per row. A source
  sustaining more writes than that can outrun the applier and lag will grow.
  Batching per transaction is the fix and is not implemented.
- **Cutover runs inside the migrating process.** Zero lag is proven by a marker
  the applier reports through an in-memory counter, which a second process
  cannot see, so there is no out-of-band `ferryman cutover`. `status` is
  catalog-only and safe to run anywhere.
- **A table with no primary key collapses exact duplicates.** Rows are matched on
  every column via `REPLICA IDENTITY FULL`, so two identical rows are
  indistinguishable and deleting one deletes both.
- **Last-write-wins needs `track_commit_timestamp = on`** on the receiving side.
  Without it, replicated changes are replayed unconditionally; the read-only
  guardrail is what keeps that from mattering.

## Prerequisites

The source needs `wal_level = logical` and enough slots and senders:

```
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8
track_commit_timestamp = on   # for last-write-wins during the rollback window
```

Tables need `REPLICA IDENTITY FULL` so `UPDATE` and `DELETE` carry a usable row
identity — mandatory for tables without a primary key, and required for TOAST
handling. `sql/schema.sql` sets it.

The target runs `wal_level = logical` too, so the reverse pipeline needs no
restart at cutover.

## Tests

Most of the suite is integration tests against two live instances. Without DSNs
they skip themselves and `go test ./...` still prints `ok`, so use the Makefile:

```sh
make test          # brings compose up, sets both DSNs, runs everything
make test-unit     # only the tests that need no database
```

CI runs the same thing and fails if any test skips, so a green run means every
test actually executed.

Running against clusters from `scripts/setup-local.sh` instead:

```sh
make test SOURCE_DSN=postgres://ferryman:ferryman@127.0.0.1:5433/ferryman \
          TARGET_DSN=postgres://ferryman:ferryman@127.0.0.1:5434/ferryman
```

## Layout

```
cmd/ferryman/     the CLI: sequences the migration and enforces its ordering
engine/
  replication.go  slot and publication setup, WAL streaming, pgoutput decoding
  applier.go      idempotent SQL generation: upserts, TOAST, composite keys
  backfill.go     exported-snapshot COPY, sliced into parallel ctid page ranges
  ddl.go          catalog reconciliation for mid-stream schema changes
  verify.go       bucketed row hashing to localise divergence
  router.go       the traffic switch, and the cutover it performs
  rollback.go     reverse pipeline setup, rollback, teardown
  conflict.go     read-only guardrail, last-write-wins, dead-letter table
sql/              schema, seed and verification fixtures
```

`CLAUDE.md` holds the design rationale, including why each simplification marked
`ponytail:` in the code was chosen over the more elaborate alternative.
