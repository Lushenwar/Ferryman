package engine

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
)

// Integration tests need the phase 0 fixture. Without a DSN they skip rather
// than fail, so `go test ./...` stays green on a machine with no postgres.
func sourceDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("FERRYMAN_SOURCE_DSN")
	if dsn == "" {
		t.Skip("FERRYMAN_SOURCE_DSN not set; skipping integration test")
	}
	return dsn
}

func TestDecodeTupleSeparatesNullFromUnchangedToast(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id"}, {Name: "email"}, {Name: "profile"},
		},
	}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
		{DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeToast},
	}}

	data, toast := decodeTuple(rel, tuple)

	if data["id"] != "7" {
		t.Errorf("id = %v, want \"7\"", data["id"])
	}
	// An explicit NULL must be present-and-nil: the target should be set to NULL.
	if v, ok := data["email"]; !ok || v != nil {
		t.Errorf("email = %v (present=%v), want present and nil", v, ok)
	}
	// An unchanged TOAST value must be absent entirely: the target must keep
	// whatever it already has. Present-and-nil here would corrupt the row.
	if _, ok := data["profile"]; ok {
		t.Error("unchanged TOAST column leaked into Data; would overwrite target with NULL")
	}
	if !toast["profile"] {
		t.Error("profile not flagged as unchanged TOAST")
	}
}

// TestStreamDecodesRowChanges is the phase 1 exit criterion: a live pgoutput
// stream from the source instance, decoded into structured events.
func TestStreamDecodesRowChanges(t *testing.T) {
	dsn := sourceDSN(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	slot := "ferryman_test_slot"
	pub := "ferryman_test_pub"

	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(context.Background())

	// Start from a clean slot so the test never inherits another run's backlog.
	_, _ = admin.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err := EnsurePublication(ctx, admin.PgConn(), pub); err != nil {
		t.Fatalf("ensure publication: %v", err)
	}

	// The slot must exist before the writes below, otherwise their WAL is
	// already gone by the time we start streaming.
	slotConn, err := ReplicationConnect(ctx, dsn)
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	defer slotConn.Close(context.Background())

	snapshot, consistent, err := EnsureSlot(ctx, slotConn, slot)
	if err != nil {
		t.Fatalf("ensure slot: %v", err)
	}
	if snapshot == "" {
		t.Fatal("no exported snapshot; phase 3 backfill would have no consistent starting point")
	}
	if consistent == 0 {
		t.Fatal("consistent point is zero")
	}
	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), dsn)
		if err == nil {
			_, _ = c.Exec(context.Background(), "SELECT pg_drop_replication_slot($1)", slot)
			_, _ = c.Exec(context.Background(), "DROP PUBLICATION IF EXISTS "+quoteIdent(pub))
			c.Close(context.Background())
		}
	})

	var userID int64
	err = admin.QueryRow(ctx, `
		INSERT INTO users (email, profile)
		VALUES ('wal-decoder@test', jsonb_build_object('blob', repeat(md5('x'), 128)))
		RETURNING id`).Scan(&userID)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	// Touching only email leaves profile out-of-line and untouched, so pgoutput
	// sends it as an unchanged-TOAST placeholder. This is the case that silently
	// nulls out large columns if the decoder gets it wrong.
	if _, err := admin.Exec(ctx, "UPDATE users SET email = 'renamed@test' WHERE id = $1", userID); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := admin.Exec(ctx, "DELETE FROM users WHERE id = $1", userID); err != nil {
		t.Fatalf("delete: %v", err)
	}

	streamCtx, stop := context.WithCancel(ctx)
	defer stop()

	var got []WALEvent
	err = Stream(streamCtx, slotConn, slot, pub, consistent, nil, func(e WALEvent) error {
		if e.Table != "users" {
			return nil // the fixture may have other traffic; ignore it
		}
		id := e.Data["id"]
		if id == nil {
			id = e.OldData["id"]
		}
		if id != strconv.FormatInt(userID, 10) {
			return nil
		}
		got = append(got, e)
		if len(got) == 3 {
			stop()
		}
		return nil
	})
	if err != nil && streamCtx.Err() == nil {
		t.Fatalf("stream: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("decoded %d events for user %d, want 3 (INSERT, UPDATE, DELETE)", len(got), userID)
	}

	ins, upd, del := got[0], got[1], got[2]
	if ins.Op != "INSERT" || upd.Op != "UPDATE" || del.Op != "DELETE" {
		t.Fatalf("ops = %s/%s/%s, want INSERT/UPDATE/DELETE", ins.Op, upd.Op, del.Op)
	}
	if ins.Data["email"] != "wal-decoder@test" {
		t.Errorf("insert email = %v", ins.Data["email"])
	}
	if ins.Data["profile"] == nil {
		t.Error("insert should carry the full profile value")
	}

	if !upd.IsToast["profile"] {
		t.Error("update did not flag profile as unchanged TOAST")
	}
	if _, present := upd.Data["profile"]; present {
		t.Error("update carries a profile value; applier would overwrite the target")
	}
	if upd.Data["email"] != "renamed@test" {
		t.Errorf("update email = %v, want renamed@test", upd.Data["email"])
	}
	// REPLICA IDENTITY FULL means the pre-image is available for keying.
	if upd.OldData["email"] != "wal-decoder@test" {
		t.Errorf("update pre-image email = %v, want wal-decoder@test", upd.OldData["email"])
	}

	if del.OldData["id"] != strconv.FormatInt(userID, 10) {
		t.Errorf("delete pre-image id = %v, want %d", del.OldData["id"], userID)
	}
	if del.OldData["email"] != "renamed@test" {
		t.Errorf("delete pre-image email = %v; full row identity missing", del.OldData["email"])
	}
}
