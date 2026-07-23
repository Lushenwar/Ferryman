package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestLastWriteWinsGuardsTheStatement checks the generated SQL, which is where
// the whole resolution lives: the comparison rides along inside the write, so
// there is no window between deciding and acting and no extra round trip.
func TestLastWriteWinsGuardsTheStatement(t *testing.T) {
	at := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

	upd := WALEvent{
		Op:      "UPDATE",
		Data:    map[string]any{"id": "1", "email": "new@test"},
		OldData: map[string]any{"id": "1", "email": "old@test"},
	}
	sql, vals, err := buildUpdate(usersMeta, upd, at)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, "pg_xact_commit_timestamp") {
		t.Errorf("UPDATE is unguarded; a newer local row would be overwritten:\n%s", sql)
	}
	// An unknown commit timestamp must lose to the replicated write. Reading it
	// the other way would drop data whenever the answer is simply unavailable —
	// on a frozen row, or with the setting off.
	if !strings.Contains(sql, "coalesce(pg_xact_commit_timestamp(xmin), '-infinity')") {
		t.Errorf("missing the -infinity fallback; an unknown row age would discard the change:\n%s", sql)
	}
	if vals[len(vals)-1] != any(at) {
		t.Errorf("commit time not bound as the last parameter: %v", vals)
	}

	del, _, err := buildDelete(usersMeta, WALEvent{Op: "DELETE", OldData: map[string]any{"id": "1"}}, at)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(del, "pg_xact_commit_timestamp") {
		t.Errorf("DELETE is unguarded:\n%s", del)
	}

	ins, _, err := buildInsert(usersMeta, WALEvent{
		Op:   "INSERT",
		Data: map[string]any{"id": "1", "email": "a@test", "profile": "{}"},
	}, at)
	if err != nil {
		t.Fatal(err)
	}
	// Inside ON CONFLICT DO UPDATE the existing row's xmin has to be qualified;
	// bare xmin does not resolve there.
	if !strings.Contains(ins, `pg_xact_commit_timestamp("users".xmin)`) {
		t.Errorf("upsert guard missing or unqualified:\n%s", ins)
	}

	// With no commit time the guard must be absent entirely, or every applier
	// that has not opted in starts requiring track_commit_timestamp.
	plain, _, err := buildUpdate(usersMeta, upd, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain, "pg_xact_commit_timestamp") {
		t.Errorf("guard leaked into an unresolved apply:\n%s", plain)
	}
}

// TestSetReadOnlyRejectsStrayWrites covers the half of conflict handling that
// stops conflicts happening rather than reconciling them afterwards.
func TestSetReadOnlyRejectsStrayWrites(t *testing.T) {
	_, targetDSN := dsns(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Registered before the flag is set: leaving a shared database read-only
	// would break every test after this one.
	t.Cleanup(func() {
		if err := SetReadOnly(context.Background(), targetDSN, false); err != nil {
			t.Errorf("could not restore writes: %v", err)
		}
	})
	if err := SetReadOnly(ctx, targetDSN, true); err != nil {
		t.Fatalf("set read-only: %v", err)
	}

	// A new session, which is what a stray client or a restarted background job
	// would open. Sessions already connected keep their old setting.
	stray, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer stray.Close(context.Background())

	if _, err := stray.Exec(ctx, "INSERT INTO audit_log (actor_id, action) VALUES (1, 'stray')"); err == nil {
		t.Error("a stray write succeeded against a read-only database")
	}

	// The applier is the one writer that must still get through. Note the
	// default_ prefix: transaction_read_only would apply only to the implicit
	// transaction that this statement is already the whole of.
	if _, err := stray.Exec(ctx, "SET default_transaction_read_only = off"); err != nil {
		t.Fatalf("opt out of read-only: %v", err)
	}
	if _, err := stray.Exec(ctx, "INSERT INTO audit_log (actor_id, action) VALUES (1, 'replicated')"); err != nil {
		t.Errorf("replication cannot write to a read-only database: %v", err)
	}
	if _, err := stray.Exec(ctx, "DELETE FROM audit_log WHERE action IN ('stray', 'replicated')"); err != nil {
		t.Fatalf("clean up: %v", err)
	}
}

// TestLastWriteWinsKeepsNewerTargetRow is the phase 9 exit criterion: a change
// replicated back from the target must not clobber a row the source wrote after
// it, and the loss has to be visible rather than silent.
func TestLastWriteWinsKeepsNewerTargetRow(t *testing.T) {
	sourceDSN, targetDSN := dsns(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	src, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	dst, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	t.Cleanup(func() {
		src.Close(context.Background())
		dst.Close(context.Background())
	})

	probeTables(ctx, t, src, dst)

	app, err := NewApplier(ctx, src, dst)
	if err != nil {
		t.Fatalf("new applier: %v", err)
	}
	if err := app.EnableConflictResolution(ctx); err != nil {
		t.Skipf("%v\n\nenable it on the target with:\n"+
			"  psql -c \"ALTER SYSTEM SET track_commit_timestamp = on\"\n"+
			"  sudo systemctl restart postgresql@16-target   # or: docker compose restart target_db", err)
	}
	t.Cleanup(func() {
		_, _ = dst.Exec(context.Background(), "DROP TABLE IF EXISTS "+quoteIdent(DLQTable))
	})
	if _, err := dst.Exec(ctx, "DELETE FROM "+quoteIdent(DLQTable)); err != nil {
		t.Fatalf("clear dlq: %v", err)
	}

	// The row is written on the target now; the replicated change claims to have
	// been committed a minute ago. The local write is newer and must survive.
	if _, err := dst.Exec(ctx, "UPDATE verify_probe SET payload = 'local' WHERE id = 7"); err != nil {
		t.Fatalf("local write: %v", err)
	}
	stale := WALEvent{
		Op:         "UPDATE",
		Schema:     "public",
		Table:      "verify_probe",
		Data:       map[string]any{"id": "7", "payload": "replicated"},
		OldData:    map[string]any{"id": "7"},
		CommitTime: time.Now().Add(-time.Minute),
	}
	if err := app.Handle(ctx)(stale); err != nil {
		t.Fatalf("apply stale change: %v", err)
	}

	var payload string
	if err := dst.QueryRow(ctx, "SELECT payload FROM verify_probe WHERE id = 7").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "local" {
		t.Errorf("payload is %q; the older replicated change overwrote a newer local write", payload)
	}
	if app.Conflicts != 1 {
		t.Errorf("Conflicts is %d, want 1", app.Conflicts)
	}

	var op, table, key string
	err = dst.QueryRow(ctx, "SELECT op, table_name, row_key::text FROM "+quoteIdent(DLQTable)).
		Scan(&op, &table, &key)
	if err != nil {
		t.Fatalf("read dlq: %v", err)
	}
	if op != "UPDATE" || !strings.Contains(key, `"id": "7"`) {
		t.Errorf("dlq entry does not identify the change: op=%s table=%s key=%s", op, table, key)
	}

	// The other direction: a change committed after the local write must land.
	fresh := stale
	fresh.CommitTime = time.Now().Add(time.Minute)
	if err := app.Handle(ctx)(fresh); err != nil {
		t.Fatalf("apply fresh change: %v", err)
	}
	if err := dst.QueryRow(ctx, "SELECT payload FROM verify_probe WHERE id = 7").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "replicated" {
		t.Errorf("payload is %q; a newer replicated change was discarded", payload)
	}
	if app.Conflicts != 1 {
		t.Errorf("Conflicts rose to %d; a winning change was counted as a conflict", app.Conflicts)
	}
}
