package engine

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	usersMeta = TableMeta{
		Schema: "public", Table: "users",
		Columns: []Column{
			{Name: "id", Type: "bigint", IsPK: true},
			{Name: "email", Type: "text"},
			{Name: "profile", Type: "jsonb"},
		},
		KeyColumns: []string{"id"},
	}
	membersMeta = TableMeta{
		Schema: "public", Table: "org_members",
		Columns: []Column{
			{Name: "org_id", Type: "bigint", IsPK: true},
			{Name: "user_id", Type: "bigint", IsPK: true},
			{Name: "role", Type: "text"},
		},
		KeyColumns: []string{"org_id", "user_id"},
	}
	auditMeta = TableMeta{
		Schema: "public", Table: "audit_log",
		Columns: []Column{
			{Name: "actor_id", Type: "bigint"},
			{Name: "action", Type: "text"},
		},
		KeyColumns: []string{"actor_id", "action"},
	}
)

func TestBuildUpdateOmitsUnchangedToastColumn(t *testing.T) {
	e := WALEvent{
		Op:      "UPDATE",
		Data:    map[string]any{"id": "1", "email": "new@test"},
		OldData: map[string]any{"id": "1", "email": "old@test"},
		IsToast: map[string]bool{"profile": true},
	}
	sql, vals, err := buildUpdate(usersMeta, e, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "profile") {
		t.Errorf("profile appears in UPDATE; target's TOASTed value would be destroyed:\n%s", sql)
	}
	if !strings.Contains(sql, `"email" = $`) {
		t.Errorf("email not being set:\n%s", sql)
	}
	// The key must come from the pre-image, since an update can move the key.
	if vals[len(vals)-1] != "1" {
		t.Errorf("key parameter = %v, want pre-image id", vals[len(vals)-1])
	}
}

// A TOAST column present in Data but flagged must still be dropped: the flag,
// not the map, is the last word.
func TestBuildUpdateDropsFlaggedColumnEvenWhenPresent(t *testing.T) {
	e := WALEvent{
		Op:      "UPDATE",
		Data:    map[string]any{"id": "1", "profile": nil},
		OldData: map[string]any{"id": "1"},
		IsToast: map[string]bool{"profile": true},
	}
	sql, _, err := buildUpdate(usersMeta, e, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "profile") {
		t.Errorf("flagged TOAST column not dropped:\n%s", sql)
	}
}

func TestBuildUpdateUsesAllCompositeKeyColumns(t *testing.T) {
	e := WALEvent{
		Op:      "UPDATE",
		Data:    map[string]any{"org_id": "7", "user_id": "9", "role": "admin"},
		OldData: map[string]any{"org_id": "7", "user_id": "9", "role": "member"},
	}
	sql, _, err := buildUpdate(membersMeta, e, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"org_id" IS NOT DISTINCT FROM`, `"user_id" IS NOT DISTINCT FROM`} {
		if !strings.Contains(sql, want) {
			t.Errorf("missing %s in composite key predicate:\n%s", want, sql)
		}
	}
}

func TestBuildInsertUpsertsOnPrimaryKey(t *testing.T) {
	e := WALEvent{Op: "INSERT", Data: map[string]any{"id": "1", "email": "a@test", "profile": "{}"}}
	sql, _, err := buildInsert(usersMeta, e, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(sql, `ON CONFLICT ("id") DO UPDATE`) {
		t.Errorf("insert is not idempotent:\n%s", sql)
	}
	if strings.Contains(sql, `"id" = EXCLUDED."id"`) {
		t.Errorf("key column should not be in the DO UPDATE set:\n%s", sql)
	}
}

func TestBuildInsertGuardsPKLessTableWithExistenceCheck(t *testing.T) {
	e := WALEvent{Op: "INSERT", Data: map[string]any{"actor_id": "1", "action": "x"}}
	sql, _, err := buildInsert(auditMeta, e, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sql, "ON CONFLICT") {
		t.Errorf("no PK exists, so there is no conflict target to use:\n%s", sql)
	}
	if !strings.Contains(sql, "WHERE NOT EXISTS") {
		t.Errorf("replay would duplicate the row:\n%s", sql)
	}
}

// A key column withheld as unchanged TOAST leaves the row unidentifiable.
// Applying anyway would match on the remaining columns and hit unrelated rows.
func TestKeyPredicateRefusesMissingKeyColumn(t *testing.T) {
	e := WALEvent{Op: "DELETE", OldData: map[string]any{"email": "a@test"}}
	if _, _, err := buildDelete(usersMeta, e, time.Time{}); err == nil {
		t.Fatal("expected an error for an event missing its key column")
	}
}

// --- integration ---

func targetPair(t *testing.T) (*pgx.Conn, context.Context) {
	t.Helper()
	dsn := os.Getenv("FERRYMAN_TARGET_DSN")
	if dsn == "" {
		t.Skip("FERRYMAN_TARGET_DSN not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn, ctx
}

// TestLoadTablesFindsRealKeyShapes is danger zone #3: the key structure has to
// come from the catalog, including the composite and PK-less cases.
func TestLoadTablesFindsRealKeyShapes(t *testing.T) {
	conn, ctx := targetPair(t)

	tables, err := LoadTables(ctx, conn)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}

	users, ok := tables["public.users"]
	if !ok {
		t.Fatal("users not discovered")
	}
	if got := strings.Join(users.KeyColumns, ","); got != "id" {
		t.Errorf("users key = %q, want id", got)
	}
	if c, _ := users.column("profile"); c.Type != "jsonb" {
		t.Errorf("profile type = %q, want jsonb", c.Type)
	}

	members := tables["public.org_members"]
	if got := strings.Join(members.KeyColumns, ","); got != "org_id,user_id" {
		t.Errorf("org_members key = %q, want the composite pair", got)
	}

	audit := tables["public.audit_log"]
	if audit.HasPK() {
		t.Error("audit_log reported a PK it does not have")
	}
	if len(audit.KeyColumns) != 3 {
		t.Errorf("PK-less table should key on all 3 columns, got %v", audit.KeyColumns)
	}
}

// TestApplyPreservesUnchangedToastValue is the phase 2 exit criterion, run
// against real postgres: an UPDATE that withholds a TOASTed column must leave
// that column's stored value byte-identical.
func TestApplyPreservesUnchangedToastValue(t *testing.T) {
	conn, ctx := targetPair(t)

	tables, err := LoadTables(ctx, conn)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}
	meta := tables["public.users"]

	// Use an explicit id well outside the fixture's range rather than the
	// sequence: the backfill resets target sequences, so relying on one here
	// would make this test depend on which other test ran first.
	const id int64 = 9900001
	if _, err := conn.Exec(ctx, "DELETE FROM users WHERE id = $1", id); err != nil {
		t.Fatalf("clear seed row: %v", err)
	}
	var originalProfile string
	err = conn.QueryRow(ctx, `
		INSERT INTO users (id, email, profile)
		VALUES ($1, 'toast-before@test', jsonb_build_object('blob', repeat(md5('toast'), 128)))
		RETURNING profile::text`, id).Scan(&originalProfile)
	if err != nil {
		t.Fatalf("seed row: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DELETE FROM users WHERE id = $1", id)
	})

	idStr := strconv.FormatInt(id, 10)
	err = Apply(ctx, conn, meta, WALEvent{
		Op:    "UPDATE",
		Table: "users",
		// profile is deliberately absent, exactly as the decoder delivers it.
		Data:    map[string]any{"id": idStr, "email": "toast-after@test"},
		OldData: map[string]any{"id": idStr, "email": "toast-before@test"},
		IsToast: map[string]bool{"profile": true},
	})
	if err != nil {
		t.Fatalf("apply update: %v", err)
	}

	var email, profile string
	if err := conn.QueryRow(ctx, "SELECT email, profile::text FROM users WHERE id = $1", id).
		Scan(&email, &profile); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if email != "toast-after@test" {
		t.Errorf("email = %q, update did not apply", email)
	}
	if profile != originalProfile {
		t.Errorf("TOASTed profile was modified: %d bytes before, %d after",
			len(originalProfile), len(profile))
	}
}

// TestApplyIsIdempotent replays each operation twice; the second application
// must be a no-op, since the stream redelivers whole transactions after a crash.
func TestApplyIsIdempotent(t *testing.T) {
	conn, ctx := targetPair(t)

	tables, err := LoadTables(ctx, conn)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}

	const orgID, userID = "999001", "999002"
	members := tables["public.org_members"]
	t.Cleanup(func() {
		_, _ = conn.Exec(context.Background(), "DELETE FROM org_members WHERE org_id = 999001")
		_, _ = conn.Exec(context.Background(), "DELETE FROM audit_log WHERE actor_id = 999003")
	})
	_, _ = conn.Exec(ctx, "DELETE FROM org_members WHERE org_id = 999001")

	ins := WALEvent{Op: "INSERT", Data: map[string]any{"org_id": orgID, "user_id": userID, "role": "member"}}
	for i := 0; i < 2; i++ {
		if err := Apply(ctx, conn, members, ins); err != nil {
			t.Fatalf("insert replay %d: %v", i, err)
		}
	}
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM org_members WHERE org_id = 999001").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("composite-key insert replayed to %d rows, want 1", n)
	}

	upd := WALEvent{
		Op:      "UPDATE",
		Data:    map[string]any{"org_id": orgID, "user_id": userID, "role": "owner"},
		OldData: map[string]any{"org_id": orgID, "user_id": userID, "role": "member"},
	}
	for i := 0; i < 2; i++ {
		if err := Apply(ctx, conn, members, upd); err != nil {
			t.Fatalf("update replay %d: %v", i, err)
		}
	}
	var role string
	if err := conn.QueryRow(ctx, "SELECT role FROM org_members WHERE org_id = 999001").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if role != "owner" {
		t.Errorf("role = %q, want owner", role)
	}

	del := WALEvent{Op: "DELETE", OldData: map[string]any{"org_id": orgID, "user_id": userID, "role": "owner"}}
	for i := 0; i < 2; i++ {
		if err := Apply(ctx, conn, members, del); err != nil {
			t.Fatalf("delete replay %d: %v", i, err)
		}
	}

	// A PK-less table has no conflict target, so its guard is the existence check.
	audit := tables["public.audit_log"]
	auditIns := WALEvent{Op: "INSERT", Data: map[string]any{
		"actor_id": "999003", "action": "replay.test", "at": "2026-01-01 00:00:00+00",
	}}
	for i := 0; i < 2; i++ {
		if err := Apply(ctx, conn, audit, auditIns); err != nil {
			t.Fatalf("pk-less insert replay %d: %v", i, err)
		}
	}
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE actor_id = 999003").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("PK-less insert replayed to %d rows, want 1", n)
	}
}
