package engine

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// dsns returns the integration DSNs, skipping the test when they are unset.
func dsns(t *testing.T) (source, target string) {
	t.Helper()
	source, target = os.Getenv("FERRYMAN_SOURCE_DSN"), os.Getenv("FERRYMAN_TARGET_DSN")
	if source == "" || target == "" {
		t.Skip("FERRYMAN_SOURCE_DSN/FERRYMAN_TARGET_DSN not set; skipping integration test")
	}
	return source, target
}

// eventually polls check until it passes or the deadline expires. Replication is
// asynchronous, so every assertion about the target is a race against the stream.
func eventually(ctx context.Context, t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s: %v", what, ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestSyncSchemaPropagatesAddedColumnMidStream is the phase 6 exit criterion.
//
// pgoutput carries no DDL, so an ALTER TABLE on the source reaches the applier
// as an INSERT naming a column the target has never heard of. The applier has
// to notice, widen the target, and replay the event — without the stream being
// restarted and without the change being lost.
func TestSyncSchemaPropagatesAddedColumnMidStream(t *testing.T) {
	sourceDSN, targetDSN := dsns(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	slot, pub := "ferryman_ddl_slot", "ferryman_ddl_pub"

	src, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	dst, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	// A separate connection for the assertions below. dst belongs to the applier
	// once the stream goroutine starts, and a *pgx.Conn carries one statement at a
	// time: polling on the same connection races the applier and fails with
	// "conn busy" whenever the poll lands mid-statement.
	probe, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect target to probe: %v", err)
	}
	// Closed via Cleanup rather than defer, and registered first: cleanups run
	// after the test body and in reverse order, so a deferred close would shut
	// these connections before the schema below is put back.
	t.Cleanup(func() {
		src.Close(context.Background())
		dst.Close(context.Background())
		probe.Close(context.Background())
	})

	// Both sides start without the column and end without it, so the test is
	// repeatable and leaves the shared fixture as it found it.
	dropColumn := func() {
		for _, c := range []*pgx.Conn{src, dst} {
			_, _ = c.Exec(context.Background(), "ALTER TABLE users DROP COLUMN IF EXISTS nickname")
		}
	}
	dropColumn()
	t.Cleanup(dropColumn)

	_, _ = src.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err := EnsurePublication(ctx, src.PgConn(), pub); err != nil {
		t.Fatalf("ensure publication: %v", err)
	}
	slotConn, err := ReplicationConnect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	defer slotConn.Close(context.Background())
	_, consistent, err := EnsureSlot(ctx, slotConn, slot)
	if err != nil {
		t.Fatalf("ensure slot: %v", err)
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), sourceDSN)
		if err != nil {
			return
		}
		defer c.Close(context.Background())
		_, _ = c.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
		_, _ = c.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+quoteIdent(pub))
	})

	// The applier caches the target's shape here, before the column exists —
	// which is the state a real migration is in when someone ships a schema
	// change halfway through.
	app, err := NewApplier(ctx, src, dst)
	if err != nil {
		t.Fatalf("new applier: %v", err)
	}

	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- Stream(streamCtx, slotConn, slot, pub, consistent, nil, app.Handle(streamCtx))
	}()

	if _, err := src.Exec(ctx, "ALTER TABLE users ADD COLUMN nickname text"); err != nil {
		t.Fatalf("alter source: %v", err)
	}
	var id int64
	err = src.QueryRow(ctx, `
		INSERT INTO users (email, profile, nickname)
		VALUES ('ddl@test', jsonb_build_object('blob', repeat(md5('ddl'), 128)), 'ferry')
		RETURNING id`).Scan(&id)
	if err != nil {
		t.Fatalf("insert using the new column: %v", err)
	}

	var nickname *string
	eventually(ctx, t, "the new column to reach the target with its value", func() bool {
		select {
		case err := <-streamErr:
			t.Fatalf("stream stopped instead of reconciling the schema: %v", err)
		default:
		}
		return probe.QueryRow(ctx, "SELECT nickname FROM users WHERE id = $1", id).Scan(&nickname) == nil
	})
	if nickname == nil || *nickname != "ferry" {
		t.Errorf("nickname on the target is %v, want \"ferry\"; the column was added but the "+
			"event was not replayed against it", nickname)
	}
}

// TestSyncSchemaRelaxesTargetOnlyNotNullColumn covers a DROP COLUMN on the
// source. The column is deliberately left in place on the target — a schema
// diff is not grounds to destroy data — but its NOT NULL has to go, or every
// later insert, which no longer carries the column, is rejected.
func TestSyncSchemaRelaxesTargetOnlyNotNullColumn(t *testing.T) {
	sourceDSN, targetDSN := dsns(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	src, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	dst, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	// Closed via Cleanup rather than defer, and registered first: cleanups run
	// after the test body and in reverse order, so a deferred close would shut
	// these connections before the schema below is put back.
	t.Cleanup(func() {
		src.Close(context.Background())
		dst.Close(context.Background())
	})

	t.Cleanup(func() {
		_, _ = dst.Exec(context.Background(), "ALTER TABLE audit_log DROP COLUMN IF EXISTS legacy")
	})
	// NOT NULL with no default is the shape that stalls inserts. Postgres needs
	// the default to add the column at all, so it is dropped straight after.
	if _, err := dst.Exec(ctx, `
		ALTER TABLE audit_log DROP COLUMN IF EXISTS legacy;
		ALTER TABLE audit_log ADD COLUMN legacy text NOT NULL DEFAULT '';
		ALTER TABLE audit_log ALTER COLUMN legacy DROP DEFAULT`); err != nil {
		t.Fatalf("set up target-only column: %v", err)
	}

	if _, err := SyncSchema(ctx, src, dst); err != nil {
		t.Fatalf("sync schema: %v", err)
	}

	var notNull, exists bool
	err = dst.QueryRow(ctx, `
		SELECT count(*) > 0, coalesce(bool_or(attnotnull), false)
		  FROM pg_attribute
		 WHERE attrelid = 'audit_log'::regclass AND attname = 'legacy' AND NOT attisdropped`).
		Scan(&exists, &notNull)
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("sync dropped the target-only column; a schema diff must not destroy data")
	}
	if notNull {
		t.Error("target-only column is still NOT NULL; inserts that omit it will be rejected")
	}
}
