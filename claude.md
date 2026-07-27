# CLAUDE.md — Zero-Downtime Migration Orchestrator (Postgres CDC)

## WORKFLOW: BRANCH + PR ONLY

No direct commits to `main`. Every change goes: `git checkout -b <branch>` → commit → `gh pr create`. A pre-commit hook (`.git/hooks/pre-commit`) enforces this locally by rejecting commits made while on `main`.

## CURRENT STATUS


```

╔══════════════════════════════════════════════════════════╗
║  BUILD PROGRESS                                 4/6 DONE ║
║  ███████████████████░░░░░░░░░  IN DEVELOPMENT            ║
║  Phase 0: Base Sync & Dual-Schema Setup         [DONE]   ║
║  Phase 1: Logical Replication & WAL Decoder     [DONE]   ║
║  Phase 2: Idempotent Applier & TOAST Handler    [DONE]   ║
║  Phase 3: Exported Snapshot Backfill Engine     [DONE]   ║
║  Phase 4: Cutover Router & Connection Switcher  [TODO]   ║
║  Phase 5: Fail-Safe Execution & Reverse CDC     [TODO]   ║
╚══════════════════════════════════════════════════════════╝

```

Phase: Technical Architecture Refinement
Status: Finalizing spec with logical replication primitives, traffic routing, and rollback strategy.
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

```