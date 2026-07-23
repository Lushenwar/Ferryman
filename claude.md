# CLAUDE.md — Zero-Downtime Migration Orchestrator (Postgres CDC)

## WORKFLOW: BRANCH + PR ONLY

No direct commits to `main`. Every change goes: `git checkout -b <branch>` → commit → `gh pr create`. A pre-commit hook (`.git/hooks/pre-commit`) enforces this locally by rejecting commits made while on `main`.

## CURRENT STATUS


```

╔══════════════════════════════════════════════════════════╗
║  BUILD PROGRESS                              10/10 DONE  ║
║  ██████████████████████████████  ALL PHASES COMPLETE     ║
║  Phase 0: Base Sync & Dual-Schema Setup         [DONE]   ║
║  Phase 1: Logical Replication & WAL Decoder     [DONE]   ║
║  Phase 2: Idempotent Applier & TOAST Handler    [DONE]   ║
║  Phase 3: Exported Snapshot Backfill Engine     [DONE]   ║
║  Phase 4: Cutover Router & Connection Switcher  [DONE]   ║
║  Phase 5: Fail-Safe Execution & Reverse CDC     [DONE]   ║
║  ── hardening ──────────────────────────────────────     ║
║  Phase 6: Schema Evolution & DDL Propagation    [DONE]   ║
║  Phase 7: Chunked Parallel Backfill             [DONE]   ║
║  Phase 8: Row-Level Consistency Auditor         [DONE]   ║
║  Phase 9: Conflict Resolution & DLQ             [DONE]   ║
╚══════════════════════════════════════════════════════════╝

```

Phase: Complete
Status: All ten phases implemented and verified against Postgres 16.
Update this as you finish each step.

## WHAT THIS FILE IS

This document is the authoritative technical specification for building the Zero-Downtime Database Migration Orchestrator. Every architectural decision, data safety boundary, WAL decoding law, and state machine transition defined here is binding. Do not deviate without explicit user approval.

---

## PRODUCT DEFINITION

The Zero-Downtime Migration Orchestrator is a lightweight, low-latency data migration engine designed to perform schema migrations between Postgres instances without taking the application offline. It combines **exported snapshot backfilling** with **Change Data Capture (CDC)** via Postgres Logical Replication (`pgoutput`) to keep a target database in sync with a source database before executing an atomic cutover with optional reverse replication.

### What the Orchestrator IS:
* A specialized migration agent that streams and decodes Postgres Write-Ahead Logs (WAL) using native `pgoutput` messages via `github.com/jackc/pglogrepl`.
* A dual-phase engine: initial exported-snapshot bulk copy followed by streaming CDC catch-up replication.
* A strict idempotency engine using dynamic primary key identification, TOAST handling, and upserts (`ON CONFLICT (...) DO UPDATE`).
* A two-way routing orchestrator that controls traffic cutover and establishes a reverse replication pipeline for clean rollbacks.

### What the Orchestrator IS NOT:
* Not a general-purpose ETL framework or Debezium clone. It does not support arbitrary non-Postgres sinks.
* Not an active-active multi-master replication tool. Replication is strictly unidirectional per phase (`Source -> Target` during sync, `Target -> Source` during rollback window).
* Not a loose script. It must handle connection drops, LSN tracking, keepalives, and crash-recovery restarts.

---

## SYSTEM ARCHITECTURE & DATA FLOW


```

```
                           ┌────────────────────────────────┐
                           │     Source Postgres DB         │
                           │  (wal_level = logical)         │
                           └──────────────┬───▲─────────────┘
                                          │   │
                               (Forward)  │   │  (Reverse Replication
                               WAL Stream │   │   on Rollback)
                                          ▼   │

```

┌───────────────────────────┐      ┌──────────┴───┴─────────────────┐
│ Exported Snapshot Engine  ├─────►│  CDC Migration Orchestrator    │
│  (SET TRANSACTION SNAPSHOT)      │  (Go Sync Engine / pglogrepl)  │
└───────────────────────────┘      └──────────────┬─────────────────┘
│
(Upserts & Replays)
│
▼
┌────────────────────────────────┐
│     Target Postgres DB         │
│   (New Schema / Partition)     │
└────────────────────────────────┘

```

<GenerateWidget title="CDC Migration Pipeline State Machine" height="700px">
{
  "widgetSpec": {
    "id": "cdc-migration-flow",
    "height": "700px",
    "prompt": "Objective: Visualize the multi-stage lifecycle of a Postgres CDC zero-downtime migration with traffic routing and rollback paths.\nData State: initialValues: { sourceRows: 100000, replicationLagMs: 450, currentStage: 'snapshot' }.\nStrategy: Standard Layout.\nLibraries: D3.js or Canvas.\nInputs:\n- Stage Selector (Snapshot Backfill, CDC Catch-Up, Verification, Cutover, Rollback)\n- Simulated Write Traffic RPS (Slider: 100 to 5000)\nBehavior: Show Source DB, Target DB, and an active Connection Router (PgBouncer/Proxy). Animate data streams showing initial exported snapshot bulk copy followed by real-time WAL event replication. Display real-time replication lag in milliseconds. When 'Cutover' is triggered, show application traffic pausing briefly, LSNs syncing to zero lag, sequences updating, and PgBouncer switching connections to Target DB. Include a 'Rollback' state where reverse CDC initializes (Target -> Source) to show safe backward synchronization."
  }
}
</GenerateWidget>

---

## CORE TECHNICAL SPECIFICATION & IMPLEMENTATION PATTERNS

### 1. Postgres Configuration Prerequisites
The source database **must** run with logical replication enabled and correct replica identity:

```sql
-- postgresql.conf
wal_level = logical
max_replication_slots = 8
max_wal_senders = 8

-- Table configuration for complete row key capture during UPDATE/DELETE
-- DEFAULT uses PK; FULL includes all old column values (required to resolve TOAST and PK-less tables)
ALTER TABLE users REPLICA IDENTITY FULL;

```

### 2. Logical Replication & Keepalive Loop (`engine/replication.go`)

Low-level logical replication streaming uses `github.com/jackc/pglogrepl`. The orchestrator handles heartbeat/keepalive responses back to Postgres to prevent connection drops and WAL retention bloat.

```go
package engine

import (
	"context"
	"time"

	"[github.com/jackc/pglogrepl](https://github.com/jackc/pglogrepl)"
	"[github.com/jackc/pgx/v5/pgconn](https://github.com/jackc/pgx/v5/pgconn)"
)

func StreamWAL(ctx context.Context, conn *pgconn.PgConn, slotName, publicationName string, startLSN pglogrepl.LSN) error {
	// Create replication slot and export snapshot
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return err
	}

	pluginArgs := []string{
		"\"proto_version\" '1'",
		"\"publication_names\" '" + publicationName + "'",
	}

	err = pglogrepl.StartReplication(ctx, conn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArgs,
	})
	if err != nil {
		return err
	}

	lastSendTime := time.Now()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			msg, err := conn.ReceiveMessage(ctx)
			if err != nil {
				return err
			}

			// Parse XLogData or handle StandbyStatusUpdate keepalive request
			switch m := msg.(type) {
			case *pgproto3.CopyData:
				if m.Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID {
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
					if err == nil && pkm.ReplyRequested {
						SendStatusUpdate(ctx, conn, startLSN, lastSendTime)
					}
				}
			}
		}
	}
}

func SendStatusUpdate(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN, t time.Time) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       t,
		ReplyRequested:   false,
	})
}

```

### 3. Native Snapshot Backfilling (`engine/snapshot.go`)

Instead of manual LSN filtering, creation of a logical replication slot exports a consistent snapshot name. The backfill engine runs inside a transaction bound to that exact LSN snapshot:

```sql
-- Transaction thread reading initial bulk state
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
SET TRANSACTION SNAPSHOT '00000003-0000001E-1';

-- Bulk COPY or SELECT from source without overlap or gap relative to WAL start LSN
COPY users TO STDOUT WITH BINARY;

COMMIT;

```

### 4. Idempotent Applier with TOAST & Composite Key Support (`engine/applier.go`)

The applier must handle composite primary keys, table-specific key columns, and unchanged TOAST attributes.

```go
package engine

import (
	"context"
	"fmt"
	"strings"

	"[github.com/jackc/pgx/v5/pgxpool](https://github.com/jackc/pgx/v5/pgxpool)"
)

type TableMetaData struct {
	TableName  string
	KeyColumns []string // Handles composite primary keys (e.g., ["org_id", "user_id"])
}

type WALEvent struct {
	Op        string                 // "INSERT", "UPDATE", "DELETE"
	Table     string
	Data      map[string]interface{} // New row state
	OldData   map[string]interface{} // Key or full old row state
	IsToast   map[string]bool        // Tracks unchanged TOAST columns
}

// ApplyUpdate generates dynamic UPSERT SQL avoiding unchanged TOAST column overwrites
func ApplyUpdate(ctx context.Context, pool *pgxpool.Pool, meta TableMetaData, event WALEvent) error {
	var setClauses []string
	var args []interface{}
	argIdx := 1

	for col, val := range event.Data {
		// DANGER ZONE GUARD: Skip unchanged TOAST columns so they are not written as NULL
		if event.IsToast[col] {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = $%d", col, argIdx))
		args = append(args, val)
		argIdx++
	}

	var keyWhere []string
	for _, keyCol := range meta.KeyColumns {
		keyVal := event.OldData[keyCol]
		if keyVal == nil {
			keyVal = event.Data[keyCol]
		}
		keyWhere = append(keyWhere, fmt.Sprintf("%s = $%d", keyCol, argIdx))
		args = append(args, keyVal)
		argIdx++
	}

	query := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		meta.TableName,
		strings.Join(setClauses, ", "),
		strings.Join(keyWhere, " AND "),
	)

	_, err := pool.Exec(ctx, query, args...)
	return err
}

```

### 5. Traffic Switcher & Connection Router (`engine/router.go`)

Traffic cutover is executed by managing connection state through an active database proxy (PgBouncer or custom app TCP router):

1. **Pause Traffic:** Send `PAUSE` signal to PgBouncer to buffer incoming client queries in memory without dropping connections.
2. **Drain Lag:** Stream remaining WAL events until `Source LSN == Target LSN`.
3. **Sync Sequences:** Execute `SELECT setval(...)` on Target for all `SERIAL` / `IDENTITY` sequences.
4. **Relocate Host:** Run `SET SERVER target_db_host` on PgBouncer and issue `RESUME`.
5. **Zero Dropped Queries:** Application clients experience a brief latency spike (the drain window) but zero failed connections.

### 6. Reverse Replication & Rollback Strategy (`engine/rollback.go`)

Once cutover completes, a **reverse CDC replication pipeline** is immediately created (`Target -> Source`).

* **Rollback Window (e.g., 60 minutes):** Writes landing on Target continue streaming back to Source in real-time.
* **Rollback Triggered:** If application metrics, errors, or data integrity checks fail on Target:
1. Pause PgBouncer.
2. Wait for Reverse CDC Lag to reach 0ms.
3. Point PgBouncer back to Source DB.
4. Resume traffic. Source is 100% current with zero lost writes.


* **Point of No Return:** Once the window expires, reverse replication is torn down and target becomes permanent primary.

---

## DANGER ZONES — TRAPS TO AVOID

1. **Missing REPLICA IDENTITY (Silent Divergence):** Default table settings only send primary keys for updates/deletes. If a table lacks a PK or has composite keys modified, `DELETE` and `UPDATE` events arrive without identifying data. **Fix:** Set `REPLICA IDENTITY FULL` on all target tables prior to initial replication.
2. **Unchanged TOAST Columns (Target Data Corruption):** Large values (text, jsonb, bytea) stored out-of-line are omitted from WAL `UPDATE` payloads if untouched. Writing these omitted fields as `NULL` or default empty values corrupts the target. **Fix:** Detect `pglogrepl` unchanged TOAST field flags and omit those columns from the SQL `SET` clause.
3. **Hardcoded Primary Keys:** Assuming every table has a single column named `id` causes runtime panics on real production schemas. **Fix:** Query `pg_index` and `pg_attribute` at startup to build a metadata map of composite primary key structures per table.
4. **Unbounded Memory Leak (Unacknowledged WAL):** Streaming WAL messages without acknowledging processed positions causes Postgres to hold WAL files on disk indefinitely, eventually filling disk space. **Fix:** Regularly send `StandbyStatusUpdate` frames with flushed LSNs in a dedicated keepalive ticker loop.
5. **Cutover Lock Deadlocks:** Attempting a cutover while long-running queries lock source tables can hang the orchestrator indefinitely. **Fix:** Enforce strict statement timeouts (`SET statement_timeout = '2s'`) on administrative cutover operations.

---

## IMPLEMENTATION PHASES

### PHASE 0: BASE SYNC & DUAL-SCHEMA SETUP

**Exit Criterion:** Spin up two isolated Postgres instances in Docker (`source_db`, `target_db`). Configure `wal_level = logical` and apply `REPLICA IDENTITY FULL` across schemas. Seed 100k+ sample rows containing PKs, composite keys, and TOASTed JSONB blobs.

### PHASE 1: LOGICAL REPLICATION & WAL DECODER

**Exit Criterion:** Establish a streaming replication connection using `pglogrepl` and `pgoutput`. Parse `INSERT`, `UPDATE`, and `DELETE` stream frames into structured Go structs and maintain a background keepalive status loop.

### PHASE 2: IDEMPOTENT APPLIER & TOAST HANDLER

**Exit Criterion:** Implement dynamic update/upsert SQL generators supporting composite primary keys. Add TOAST column detection logic to verify that updates on tables with unchanged TOAST values preserve original target data without writing `NULL`.

### PHASE 3: EXPORTED SNAPSHOT BACKFILL ENGINE

**Exit Criterion:** Implement the exported snapshot reader (`SET TRANSACTION SNAPSHOT`). Execute a concurrent initial snapshot backfill while logical replication streams in parallel, verifying zero data loss and no duplicate insert key conflicts.

### PHASE 4: CUTOVER ROUTER & CONNECTION SWITCHER

**Exit Criterion:** Integrate a PgBouncer admin client or dynamic TCP proxy layer. Test an automated cutover: pause connections, drain remaining replication lag to 0ms, update sequences, and switch proxy endpoints to target without client connection drops.

### PHASE 5: FAIL-SAFE EXECUTION & REVERSE CDC

**Exit Criterion:** Provision an automated reverse replication pipeline (`Target -> Source`) immediately upon successful forward cutover. Simulate a target failure within the rollback window, trigger the rollback command, and confirm all post-cutover writes stream back to source without data loss.

---

## HARDENING PHASES 6–9

Four gaps found in the phase 0–5 engine. Each is recorded with the problem, the proposed fix, what the codebase already does about it, and the verdict on how much of the proposal is worth building. Deliberate reductions are marked `ponytail:` in the code.

### PHASE 6: SCHEMA EVOLUTION & DDL PROPAGATION

**Problem:** `pgoutput` streams DML only. A developer running `ALTER TABLE ... ADD COLUMN` on the source mid-migration produces WAL frames carrying a column the target does not have.

**What we already do:** nothing. The publication is `FOR ALL TABLES` so new columns arrive in the stream; `LoadTables` is called once at startup and never refreshed. The failure is loud, not silent — the applier gets SQLSTATE 42703, `Stream` returns without acking, and the slot replays from the last commit. No corruption, but the migration stalls.

**Proposed:** source-side event triggers `ON ddl_command_end`, captured, translated, validated, applied ahead of subsequent DML.

**Verdict — build a reduced version.** Event triggers are the wrong mechanism here: they need superuser on the source, a capture table, an out-of-band shipping channel, and then a hand-built ordering guarantee against the WAL stream. The WAL stream already carries the DDL signal, in band and perfectly ordered — pgoutput emits a fresh `RelationMessage` before the first DML on a changed relation, so the DML itself is the notification, arriving exactly where it must be handled.

`SyncSchema` therefore diffs the two catalogs directly and the applier calls it on 42703, then retries once. Ordering is free, no superuser is required, and the fast path costs nothing because it only runs after a failure. `ALTER TABLE` on the source stays a normal `ALTER TABLE`.

Scope of the reduction: additive column reconciliation, plus dropping `NOT NULL` on target-only columns so a source-side `DROP COLUMN` does not stall inserts. Missing *tables* are not created — that pulls in indexes, constraints, sequences and identity columns — so the applier fails loudly on 42P01 with an instruction instead.

**Exit Criterion:** `ALTER TABLE ... ADD COLUMN` on the source mid-stream; the subsequent INSERT lands on the target with the new column populated, without restarting the stream.

### PHASE 7: CHUNKED PARALLEL BACKFILL

**Problem:** one `COPY` per table over one connection. A single multi-terabyte table pins the whole backfill to one core and one socket regardless of parallelism.

**What we already do:** `Backfill` parallelises *across* tables via `errgroup` with a limit, and streams each table through an `io.Pipe` so table size is not bounded by memory. Skew is the gap: N-1 workers idle while the largest table finishes alone.

**Proposed:** query min/max PK, divide into balanced key ranges, one worker per range under the shared exported snapshot.

**Verdict — build it, but slice on `ctid`, not on the primary key.** PK range slicing needs a single-column ordered key (excludes `org_members`' composite key and `audit_log`'s absent one), a min/max probe per table, and it balances by key distribution rather than by bytes — a table with a sparse or clustered key produces wildly uneven chunks. TID range scans (PG12+) slice the physical heap: bounds come from `pg_class.relpages` with no probe query, every table qualifies whatever its key, and chunks are balanced by the thing that actually determines COPY time, which is pages read. `ctid` is stable for the life of the backfill because the `REPEATABLE READ` snapshot transactions hold `ACCESS SHARE`, which blocks the `VACUUM FULL`/`CLUSTER` that would rewrite the heap.

**Exit Criterion:** a table backfills correctly when split across N workers, producing byte-identical output to the single-worker path.

### PHASE 8: ROW-LEVEL CONSISTENCY AUDITOR

**Problem:** proving source and target match before committing to the cutover.

**What we already do:** cutover correctness is guaranteed *by construction* — `emitCutoverMarker` drops a logical decoding message into the stream and `waitForLSN` blocks until the applier passes it, so everything committed before the marker is applied by definition. The tests assert whole-table `md5(string_agg(...))` equality. What is missing is a runtime check, and a way to localise a mismatch when one does appear.

**Proposed:** Merkle trees over chunked row hashes, bisecting to isolate diverging rows without moving full tables.

**Verdict — build one Merkle level, not a tree.** A tree's advantage is logarithmic bisection over a slow link. Here a single `GROUP BY` returns every bucket hash in one round trip per side, so the first round already reduces the comparison from rows to hashes *and* names the diverging buckets. Interior nodes would save round trips that are not being made.

Phase 7's `ctid` chunker is explicitly **not** reused: physical page ranges partition the source's heap, and the target's heap holds the same rows in a completely different physical order, so identical `ctid` bounds would compare unrelated row sets. Buckets are logical instead — `md5` of the row's key columns, mod N — which both sides compute identically and which keeps a modified row in the same bucket on both sides, so a mismatch localises. `md5` rather than `hashtext` because a migration between two Postgres major versions must still agree on the partition.

Comparing text renderings across two clusters also means pinning `TimeZone`, `DateStyle` and `extra_float_digits` on both connections, or identical rows hash differently for reasons that have nothing to do with the data.

Placement matters more than the algorithm: source and target are *never* equal while CDC is live, because lag is not a defect. The only sound moment to compare is inside the cutover, after the marker drain and before the router repoints — traffic paused, both databases quiescent. `CutoverConfig.Verify` runs it there and aborts the cutover on mismatch, resuming traffic against the source.

**Exit Criterion:** a deliberately corrupted target row is detected and localised to its chunk, and a cutover with `Verify` set aborts rather than switching.

### PHASE 9: CONFLICT RESOLUTION & DLQ

**Problem:** during the reverse CDC window a stray writer on the source collides with replicated writes coming back from the target.

**What we already do:** the `Router` is the only path to the database, and it is paused during the switch — a client that reaches the drained side is bypassing the router, which is a network-topology problem rather than a replication one.

**Proposed:** last-write-wins on `track_commit_timestamp`, source/target priority rules, a dead-letter queue with an admin review endpoint.

**Verdict — prevention first, then LWW and a DLQ table; no endpoint, no priority modes.** The cheap and total fix is to stop the drained side accepting writes at all: `SetReadOnly` flips `default_transaction_read_only` on the database, which turns the conflict into a rejected write on the writer that should not exist. That is a handful of lines and removes the entire class.

For what it cannot cover — a superuser, or a background job that sets its own `transaction_read_only` — LWW is genuinely cheap: `track_commit_timestamp = on` makes `pg_xact_commit_timestamp(xmin)` available, so the applier compares the incoming event's commit time against the existing row's and skips the older write. `Conflicts` counts what it skipped.

The DLQ is a table and an insert. The admin review endpoint is an HTTP server, an auth story and a UI for a scenario `SetReadOnly` already prevents — `SELECT * FROM ferryman_dlq` is the review interface until someone needs more. Priority modes are one field that is never set to anything but its default; LWW is the rule.

**Exit Criterion:** with `track_commit_timestamp` on, a source-side write newer than a replicated one survives the replay and is recorded as a conflict; `SetReadOnly` rejects the stray write outright.

```