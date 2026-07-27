package engine

import (
	"context"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// hostPort pulls the address out of a DSN so the router can dial it directly.
func hostPort(t *testing.T, dsn string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	return u.Host
}

// dsnVia rewrites a DSN to connect through the router instead of the database.
func dsnVia(t *testing.T, dsn, addr string) string {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.Host = addr
	return u.String()
}

func TestRouterHoldsConnectionsWhilePausedInsteadOfRefusing(t *testing.T) {
	// A plain TCP echo backend keeps this test about the router alone.
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		for {
			c, err := backend.Accept()
			if err != nil {
				return
			}
			go func() { defer c.Close(); _, _ = c.Write([]byte("ok")) }()
		}
	}()

	r, err := NewRouter("127.0.0.1:0", backend.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	go r.Serve()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.Pause(ctx); err != nil {
		t.Fatalf("pause: %v", err)
	}

	// Dial while paused. The connection must be accepted and held; a refusal
	// here is the failure the whole design is meant to prevent.
	dialed := make(chan error, 1)
	go func() {
		c, err := net.Dial("tcp", r.Addr())
		if err != nil {
			dialed <- err
			return
		}
		defer c.Close()
		buf := make([]byte, 2)
		_, err = c.Read(buf)
		dialed <- err
	}()

	select {
	case err := <-dialed:
		t.Fatalf("paused router completed a connection instead of holding it (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	r.Resume()
	select {
	case err := <-dialed:
		if err != nil {
			t.Fatalf("held connection failed after resume: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("held connection was never released after resume")
	}
}

// TestCutoverSwitchesTrafficWithoutDroppingConnections is the phase 4 exit
// criterion. Live traffic runs through the router the whole time; the cutover
// pauses it, drains replication lag to zero, syncs sequences and repoints the
// backend. Not one client connection may fail.
func TestCutoverSwitchesTrafficWithoutDroppingConnections(t *testing.T) {
	sourceDSN := os.Getenv("FERRYMAN_SOURCE_DSN")
	targetDSN := os.Getenv("FERRYMAN_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("FERRYMAN_SOURCE_DSN/FERRYMAN_TARGET_DSN not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	slot, pub := "ferryman_cutover_slot", "ferryman_cutover_pub"
	const probeAction = "cutover.probe"

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

	for _, c := range []*pgx.Conn{src, dst} {
		if _, err := c.Exec(ctx, "DELETE FROM audit_log WHERE action = $1", probeAction); err != nil {
			t.Fatalf("clear probes: %v", err)
		}
	}
	if _, err := dst.Exec(ctx, "TRUNCATE users, org_members, audit_log"); err != nil {
		t.Fatalf("clear target: %v", err)
	}
	_, _ = src.Exec(ctx, "SELECT pg_drop_replication_slot($1)", slot)
	if err := EnsurePublication(ctx, src.PgConn(), pub); err != nil {
		t.Fatalf("ensure publication: %v", err)
	}

	slotConn, err := ReplicationConnect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("replication connect: %v", err)
	}
	defer slotConn.Close(context.Background())

	snapshot, consistent, err := EnsureSlot(ctx, slotConn, slot)
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

	meta, err := LoadTables(ctx, src)
	if err != nil {
		t.Fatalf("load tables: %v", err)
	}
	metas := []TableMeta{meta["public.users"], meta["public.org_members"], meta["public.audit_log"]}
	if err := Backfill(ctx, sourceDSN, targetDSN, snapshot, metas, 4); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	// Replication runs for the rest of the test; cutover waits on its progress.
	applyConn, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect applier: %v", err)
	}
	defer applyConn.Close(context.Background())

	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	var progress Progress
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- Stream(streamCtx, slotConn, slot, pub, consistent, &progress, func(e WALEvent) error {
			m, ok := meta[e.Schema+"."+e.Table]
			if !ok {
				return nil
			}
			return Apply(streamCtx, applyConn, m, e)
		})
	}()

	r, err := NewRouter("127.0.0.1:0", hostPort(t, sourceDSN))
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer r.Close()
	go r.Serve()
	routedDSN := dsnVia(t, sourceDSN, r.Addr())

	// Client traffic: a fresh connection per write, as a pooled application
	// would do. Any failure at all fails the test.
	var (
		wg          sync.WaitGroup
		writes      atomic.Int64
		failures    atomic.Int64
		firstErr    error
		firstErrMu  sync.Mutex
		stopTraffic = make(chan struct{})
	)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopTraffic:
				return
			default:
			}
			err := func() error {
				c, err := pgx.Connect(ctx, routedDSN)
				if err != nil {
					return err
				}
				defer c.Close(context.Background())
				_, err = c.Exec(ctx, "INSERT INTO audit_log (actor_id, action) VALUES ($1, $2)",
					writes.Load()+1, probeAction)
				return err
			}()
			if err != nil {
				failures.Add(1)
				firstErrMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				firstErrMu.Unlock()
				continue
			}
			writes.Add(1)
		}
	}()

	// Let traffic build up against the source.
	time.Sleep(1500 * time.Millisecond)
	if writes.Load() == 0 {
		t.Fatal("no traffic reached the source before cutover")
	}

	start := time.Now()
	err = Cutover(ctx, CutoverConfig{
		Router:        r,
		Progress:      &progress,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		TargetBackend: hostPort(t, targetDSN),
		DrainTimeout:  30 * time.Second,
	})
	if err != nil {
		t.Fatalf("cutover: %v", err)
	}
	writesAtCutover := writes.Load()
	t.Logf("cutover completed in %s after %d writes", time.Since(start).Round(time.Millisecond), writesAtCutover)

	if r.Backend() != hostPort(t, targetDSN) {
		t.Fatalf("router still points at %s", r.Backend())
	}

	// Traffic must keep flowing against the new backend.
	time.Sleep(1500 * time.Millisecond)
	close(stopTraffic)
	wg.Wait()
	stopStream()

	if n := failures.Load(); n != 0 {
		t.Errorf("%d client connections failed during cutover; first error: %v", n, firstErr)
	}
	totalWrites := writes.Load()
	if totalWrites <= writesAtCutover {
		t.Fatalf("no writes landed after cutover: %d total vs %d at cutover", totalWrites, writesAtCutover)
	}

	// Everything written before the switch had to reach the target before it
	// happened — that is what draining lag to zero means.
	var srcProbes, dstProbes int64
	if err := src.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE action = $1", probeAction).Scan(&srcProbes); err != nil {
		t.Fatal(err)
	}
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE action = $1", probeAction).Scan(&dstProbes); err != nil {
		t.Fatal(err)
	}
	if srcProbes != writesAtCutover {
		t.Errorf("source holds %d pre-cutover writes, expected %d", srcProbes, writesAtCutover)
	}
	// Post-cutover writes go straight to the target and are not replicated back
	// until phase 5, so the target holds every write from both sides.
	if dstProbes != totalWrites {
		t.Errorf("target holds %d writes, expected all %d (%d replicated + %d direct)",
			dstProbes, totalWrites, writesAtCutover, totalWrites-writesAtCutover)
	}

	// Sequences must be usable on the target immediately after cutover.
	var seqLast, maxID int64
	if err := dst.QueryRow(ctx, "SELECT last_value FROM users_id_seq").Scan(&seqLast); err != nil {
		t.Fatal(err)
	}
	if err := dst.QueryRow(ctx, "SELECT coalesce(max(id), 0) FROM users").Scan(&maxID); err != nil {
		t.Fatal(err)
	}
	if seqLast < maxID {
		t.Errorf("users_id_seq at %d but max id is %d; the first insert after cutover would collide", seqLast, maxID)
	}
}
