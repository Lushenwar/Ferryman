package engine

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func checksum(ctx context.Context, t *testing.T, conn *pgx.Conn, table string) (int64, string) {
	t.Helper()
	var n int64
	var sum string
	if err := conn.QueryRow(ctx, "SELECT count(*), coalesce(md5(string_agg(t::text, '' ORDER BY t::text)), '') FROM "+table+" t").
		Scan(&n, &sum); err != nil {
		t.Fatalf("checksum %s: %v", table, err)
	}
	return n, sum
}

// TestBackfillAndCDCConverge is the phase 3 exit criterion.
//
// It exports a snapshot, writes to the source *after* that snapshot is taken,
// backfills, and checks the post-snapshot writes are absent — proving the copy
// really is bounded at the slot's LSN rather than racing live traffic. It then
// replays the WAL from that same LSN and checks source and target become
// byte-identical: no gap, no overlap, no duplicate key conflicts.
func TestBackfillAndCDCConverge(t *testing.T) {
	sourceDSN := os.Getenv("FERRYMAN_SOURCE_DSN")
	targetDSN := os.Getenv("FERRYMAN_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("FERRYMAN_SOURCE_DSN/FERRYMAN_TARGET_DSN not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tablesToCopy := []string{"public.users", "public.org_members", "public.audit_log"}
	slot, pub := "ferryman_backfill_slot", "ferryman_backfill_pub"

	src, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect source: %v", err)
	}
	defer src.Close(context.Background())
	dst, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect target: %v", err)
	}
	defer dst.Close(context.Background())

	if _, err := dst.Exec(ctx, "TRUNCATE users, org_members, audit_log"); err != nil {
		t.Fatalf("clear target: %v", err)
	}
	// Seed the rows this test mutates, before the snapshot is taken. Relying on
	// the shared fixture would make the test pass only on a pristine database
	// and fail on its own second run.
	const testOrg, testUser = 990001, 990042
	if _, err := src.Exec(ctx, `
		INSERT INTO org_members (org_id, user_id, role)
		VALUES ($1, $2, 'member'), ($1 + 1, $2, 'admin')
		ON CONFLICT (org_id, user_id) DO UPDATE SET role = EXCLUDED.role`, testOrg, testUser); err != nil {
		t.Fatalf("seed memberships: %v", err)
	}

	_, _ = src.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err := EnsurePublication(ctx, src.PgConn(), pub); err != nil {
		t.Fatalf("ensure publication: %v", err)
	}

	// This connection exports the snapshot and must outlive the backfill:
	// postgres drops an exported snapshot when its exporting session ends.
	slotConn, err := ReplicationConnect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	defer slotConn.Close(context.Background())

	snapshot, consistent, err := EnsureSlot(ctx, slotConn, slot)
	if err != nil {
		t.Fatalf("ensure slot: %v", err)
	}
	if snapshot == "" {
		t.Fatal("slot exported no snapshot")
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

	// Traffic that lands after the snapshot. None of it may appear in the copy;
	// all of it must arrive later over CDC.
	var newUserID int64
	err = src.QueryRow(ctx, `
		INSERT INTO users (email, profile)
		VALUES ('post-snapshot@test', jsonb_build_object('blob', repeat(md5('post'), 128)))
		RETURNING id`).Scan(&newUserID)
	if err != nil {
		t.Fatalf("post-snapshot insert: %v", err)
	}
	// Touch only email so profile stays out-of-line and is withheld from the
	// WAL frame — the case that silently nulls large columns if mishandled.
	// The new value is unique per run: a fixed one would already equal the
	// snapshot value on a re-run, making the isolation check below vacuous.
	var emailInSnapshot string
	if err := src.QueryRow(ctx, "SELECT email FROM users WHERE id = 1").Scan(&emailInSnapshot); err != nil {
		t.Fatal(err)
	}
	renamedEmail := fmt.Sprintf("cdc-renamed-%d@test", time.Now().UnixNano())
	if _, err := src.Exec(ctx, "UPDATE users SET email = $1 WHERE id = 1", renamedEmail); err != nil {
		t.Fatalf("post-snapshot update: %v", err)
	}
	// The seed gives most users more than one membership, so record how many
	// rows the snapshot should still hold rather than assuming a single row.
	var membershipsBeforeDelete int
	if err := src.QueryRow(ctx, "SELECT count(*) FROM org_members WHERE user_id = 990042").
		Scan(&membershipsBeforeDelete); err != nil {
		t.Fatal(err)
	}
	if membershipsBeforeDelete == 0 {
		t.Fatal("fixture has no memberships for the seeded test user; nothing to delete")
	}
	if _, err := src.Exec(ctx, "DELETE FROM org_members WHERE user_id = 990042"); err != nil {
		t.Fatalf("post-snapshot delete: %v", err)
	}
	if _, err := src.Exec(ctx, "INSERT INTO audit_log (actor_id, action) VALUES (7, 'post.snapshot')"); err != nil {
		t.Fatalf("post-snapshot audit insert: %v", err)
	}

	meta, err := LoadTables(ctx, src)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}
	var metas []TableMeta
	for _, name := range tablesToCopy {
		metas = append(metas, meta[name])
	}

	start := time.Now()
	if err := Backfill(ctx, sourceDSN, targetDSN, snapshot, metas, 4); err != nil {
		t.Fatalf("backfill: %v", err)
	}
	t.Logf("backfilled %d tables in %s", len(metas), time.Since(start).Round(time.Millisecond))

	// The snapshot bounds the copy: post-snapshot traffic must be invisible.
	var n int
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM users WHERE id = $1", newUserID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("backfill copied a row committed after the snapshot; the copy is racing live traffic")
	}
	var email string
	if err := dst.QueryRow(ctx, "SELECT email FROM users WHERE id = 1").Scan(&email); err != nil {
		t.Fatal(err)
	}
	if email != emailInSnapshot {
		t.Errorf("users.id=1 email is %q in the copy but was %q in the snapshot; "+
			"the backfill picked up a post-snapshot update", email, emailInSnapshot)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM org_members WHERE user_id = 990042").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != membershipsBeforeDelete {
		t.Errorf("post-snapshot delete leaked into the copy: %d rows, want the %d rows the snapshot held",
			n, membershipsBeforeDelete)
	}

	// The copy writes explicit ids, so without a sequence sync the target's
	// next generated id would collide with a row that was just copied.
	var seqLast, maxID int64
	if err := dst.QueryRow(ctx, "SELECT last_value FROM users_id_seq").Scan(&seqLast); err != nil {
		t.Fatal(err)
	}
	if err := dst.QueryRow(ctx, "SELECT coalesce(max(id), 0) FROM users").Scan(&maxID); err != nil {
		t.Fatal(err)
	}
	if seqLast < maxID {
		t.Errorf("users_id_seq is at %d but the highest copied id is %d; "+
			"the next insert on the target would collide", seqLast, maxID)
	}

	// Now replay the WAL held since the slot's consistent point.
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()

	applyConn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect applier: %v", err)
	}
	defer applyConn.Close(context.Background())

	streamErr := make(chan error, 1)
	go func() {
		streamErr <- Stream(streamCtx, slotConn, slot, pub, consistent, nil, func(e WALEvent) error {
			m, ok := meta[e.Schema+"."+e.Table]
			if !ok {
				return nil
			}
			return Apply(streamCtx, applyConn, m, e)
		})
	}()

	// Source is quiet now, so convergence is a stable end state, not a race.
	deadline := time.Now().Add(90 * time.Second)
	var mismatch string
	for time.Now().Before(deadline) {
		mismatch = ""
		for _, table := range tablesToCopy {
			sn, ss := checksum(ctx, t, src, table)
			dn, ds := checksum(ctx, t, dst, table)
			if sn != dn || ss != ds {
				mismatch = table
				break
			}
		}
		if mismatch == "" {
			break
		}
		select {
		case err := <-streamErr:
			t.Fatalf("stream stopped early: %v", err)
		case <-time.After(250 * time.Millisecond):
		}
	}
	stopStream()

	if mismatch != "" {
		sn, ss := checksum(ctx, t, src, mismatch)
		dn, ds := checksum(ctx, t, dst, mismatch)
		t.Fatalf("%s never converged: source %d rows/%s, target %d rows/%s", mismatch, sn, ss, dn, ds)
	}

	// Spot-check the two cases that fail silently rather than loudly.
	var profileLen int
	if err := dst.QueryRow(ctx, "SELECT length(profile::text) FROM users WHERE id = 1").Scan(&profileLen); err != nil {
		t.Fatal(err)
	}
	if profileLen < 2000 {
		t.Errorf("users.profile is %d bytes after a TOAST-withholding update; value was destroyed", profileLen)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM users WHERE id = $1", newUserID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("post-snapshot insert did not arrive over CDC: %d rows", n)
	}
}
