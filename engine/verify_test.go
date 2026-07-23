package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// probeTables builds an identical table on both sides, seeded identically, and
// returns its metadata.
//
// The shared fixture is not usable here: earlier tests apply events straight to
// the target, so source and target legitimately differ by the time this runs
// and a verifier that found nothing would be indistinguishable from one that
// found everything.
func probeTables(ctx context.Context, t *testing.T, src, dst *pgx.Conn) TableMeta {
	t.Helper()
	const ddl = `
		DROP TABLE IF EXISTS verify_probe;
		CREATE TABLE verify_probe (id bigint PRIMARY KEY, payload text NOT NULL);
		INSERT INTO verify_probe SELECT g, md5(g::text) FROM generate_series(1, 500) g`
	for _, c := range []*pgx.Conn{src, dst} {
		if _, err := c.Exec(ctx, ddl); err != nil {
			t.Fatalf("build probe table: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, c := range []*pgx.Conn{src, dst} {
			_, _ = c.Exec(context.Background(), "DROP TABLE IF EXISTS verify_probe")
		}
	})

	tables, err := LoadTables(ctx, dst)
	if err != nil {
		t.Fatalf("load probe metadata: %v", err)
	}
	meta, ok := tables["public.verify_probe"]
	if !ok {
		t.Fatal("probe table missing from the catalog")
	}
	return meta
}

// TestVerifyLocalisesADivergedRow is half the phase 8 exit criterion: a single
// wrong value in half a thousand rows has to be found, and found in a slice
// small enough to be worth drilling into.
func TestVerifyLocalisesADivergedRow(t *testing.T) {
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

	meta := probeTables(ctx, t, src, dst)

	// Identical to begin with. A verifier that cannot tell equal tables apart
	// from unequal ones is worse than none.
	bad, err := Verify(ctx, sourceDSN, targetDSN, []TableMeta{meta}, DefaultVerifyBuckets)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(bad) != 0 {
		t.Fatalf("identical tables reported as diverging: %v", bad)
	}

	const corrupted = 271
	if _, err := dst.Exec(ctx, "UPDATE verify_probe SET payload = 'wrong' WHERE id = $1", corrupted); err != nil {
		t.Fatalf("corrupt target row: %v", err)
	}

	bad, err = Verify(ctx, sourceDSN, targetDSN, []TableMeta{meta}, DefaultVerifyBuckets)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if len(bad) != 1 {
		t.Fatalf("one wrong row produced %d mismatched buckets, want 1: %v", len(bad), bad)
	}
	m := bad[0]
	// The row count is unchanged, so only the hash can give it away.
	if m.SourceRows != m.TargetRows {
		t.Errorf("row counts differ (%d vs %d); an edited row should stay in its bucket",
			m.SourceRows, m.TargetRows)
	}

	// The reported filter has to actually select the offending row, or
	// "localised" means nothing.
	var inBucket bool
	if err := dst.QueryRow(ctx,
		"SELECT count(*) = 1 FROM verify_probe WHERE id = $1 AND "+m.Filter, corrupted).Scan(&inBucket); err != nil {
		t.Fatalf("apply reported filter: %v", err)
	}
	if !inBucket {
		t.Errorf("bucket %d does not contain the row that was corrupted; filter: %s", m.Bucket, m.Filter)
	}
	var bucketRows int
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM verify_probe WHERE "+m.Filter).Scan(&bucketRows); err != nil {
		t.Fatal(err)
	}
	if bucketRows >= 500 {
		t.Errorf("bucket holds all %d rows; the partition is not narrowing anything down", bucketRows)
	}
	t.Logf("localised 1 diverged row out of 500 to a bucket of %d", bucketRows)
}

// TestCutoverAbortsWhenVerifyFindsDivergence is the other half: a cutover that
// notices the target is wrong must leave traffic where it is. Switching anyway
// and reporting the problem afterwards would move production onto a database
// already known to be bad.
func TestCutoverAbortsWhenVerifyFindsDivergence(t *testing.T) {
	sourceDSN, targetDSN := dsns(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
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

	meta := probeTables(ctx, t, src, dst)
	if _, err := dst.Exec(ctx, "DELETE FROM verify_probe WHERE id = 42"); err != nil {
		t.Fatalf("diverge the target: %v", err)
	}

	slot, pub := "ferryman_verify_slot", "ferryman_verify_pub"
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

	// A stream that applies nothing still advances Progress at every commit,
	// which is all the marker drain reads. The divergence therefore survives to
	// the point where Verify is meant to catch it.
	var progress Progress
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	go func() {
		_ = Stream(streamCtx, slotConn, slot, pub, consistent, &progress, func(WALEvent) error { return nil })
	}()

	router, err := NewRouter("127.0.0.1:0", "source.invalid:5432")
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer router.Close()
	go router.Serve()

	err = Cutover(ctx, CutoverConfig{
		Router:        router,
		Progress:      &progress,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		TargetBackend: "target.invalid:5432",
		DrainTimeout:  60 * time.Second,
		Verify:        []TableMeta{meta},
	})
	if err == nil {
		t.Fatal("cutover succeeded against a target that is missing a row")
	}
	t.Logf("cutover refused: %v", err)

	if got := router.Backend(); got != "source.invalid:5432" {
		t.Errorf("traffic was repointed to %q despite the abort; it must stay on the source", got)
	}
}
