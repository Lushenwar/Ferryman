package engine

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// probeWriter drives continuous client traffic through the router, one fresh
// connection per write, and records any failure.
type probeWriter struct {
	writes   atomic.Int64
	failures atomic.Int64

	mu       sync.Mutex
	firstErr error

	stop chan struct{}
	done sync.WaitGroup
}

func startProbeWriter(ctx context.Context, dsn, action string) *probeWriter {
	p := &probeWriter{stop: make(chan struct{})}
	p.done.Add(1)
	go func() {
		defer p.done.Done()
		for {
			select {
			case <-p.stop:
				return
			default:
			}
			err := func() error {
				c, err := pgx.Connect(ctx, dsn)
				if err != nil {
					return err
				}
				defer c.Close(context.Background())
				_, err = c.Exec(ctx, "INSERT INTO audit_log (actor_id, action) VALUES ($1, $2)",
					p.writes.Load()+1, action)
				return err
			}()
			if err != nil {
				p.failures.Add(1)
				p.mu.Lock()
				if p.firstErr == nil {
					p.firstErr = err
				}
				p.mu.Unlock()
				continue
			}
			p.writes.Add(1)
		}
	}()
	return p
}

func (p *probeWriter) Stop() {
	close(p.stop)
	p.done.Wait()
}

// TestRollbackReturnsPostCutoverWritesToSource is the phase 5 exit criterion.
//
// It runs the whole lifecycle: backfill, forward CDC, cutover to the target,
// reverse CDC, then a rollback inside the window. Every write made while the
// target was authoritative must be back on the source afterwards, and no client
// connection may fail through either switch.
func TestRollbackReturnsPostCutoverWritesToSource(t *testing.T) {
	sourceDSN := os.Getenv("FERRYMAN_SOURCE_DSN")
	targetDSN := os.Getenv("FERRYMAN_TARGET_DSN")
	if sourceDSN == "" || targetDSN == "" {
		t.Skip("FERRYMAN_SOURCE_DSN/FERRYMAN_TARGET_DSN not set; skipping integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	const probeAction = "rollback.probe"
	fwdSlot, fwdPub := "ferryman_fwd_slot", "ferryman_fwd_pub"
	revSlot, revPub := "ferryman_rev_slot", "ferryman_rev_pub"

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
	_, _ = src.Exec(ctx, "SELECT pg_drop_replication_slot($1)", fwdSlot)
	_, _ = dst.Exec(ctx, "SELECT pg_drop_replication_slot($1)", revSlot)
	t.Cleanup(func() {
		bg := context.Background()
		c1, err := pgx.Connect(bg, sourceDSN)
		if err == nil {
			_, _ = c1.Exec(bg, "SELECT pg_drop_replication_slot($1)", fwdSlot)
			_, _ = c1.Exec(bg, "DROP PUBLICATION IF EXISTS "+quoteIdent(fwdPub))
			c1.Close(bg)
		}
		_ = TeardownReverse(bg, targetDSN, revSlot, revPub)
	})

	// --- forward pipeline ---
	if err := EnsurePublication(ctx, src.PgConn(), fwdPub); err != nil {
		t.Fatalf("ensure forward publication: %v", err)
	}
	fwdConn, err := ReplicationConnect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("forward replication connect: %v", err)
	}
	defer fwdConn.Close(context.Background())
	snapshot, consistent, err := EnsureSlot(ctx, fwdConn, fwdSlot)
	if err != nil {
		t.Fatalf("ensure forward slot: %v", err)
	}

	sourceMeta, err := LoadTables(ctx, src)
	if err != nil {
		t.Fatalf("load source tables: %v", err)
	}
	metas := []TableMeta{sourceMeta["public.users"], sourceMeta["public.org_members"], sourceMeta["public.audit_log"]}
	if err := Backfill(ctx, sourceDSN, targetDSN, snapshot, metas, 4); err != nil {
		t.Fatalf("backfill: %v", err)
	}

	fwdApply, err := pgx.Connect(ctx, targetDSN)
	if err != nil {
		t.Fatalf("connect forward applier: %v", err)
	}
	defer fwdApply.Close(context.Background())

	fwdCtx, stopForward := context.WithCancel(ctx)
	defer stopForward()
	var fwdProgress Progress
	go func() {
		_ = Stream(fwdCtx, fwdConn, fwdSlot, fwdPub, consistent, &fwdProgress, func(e WALEvent) error {
			m, ok := sourceMeta[e.Schema+"."+e.Table]
			if !ok {
				return nil
			}
			return Apply(fwdCtx, fwdApply, m, e)
		})
	}()

	// The reverse slot is created before cutover, so no write landing on the
	// target immediately afterwards can escape being captured.
	revConn, revStart, err := PrepareReverse(ctx, targetDSN, revSlot, revPub)
	if err != nil {
		t.Fatalf("prepare reverse: %v", err)
	}
	defer revConn.Close(context.Background())

	r, err := NewRouter("127.0.0.1:0", hostPort(t, sourceDSN))
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	defer r.Close()
	go r.Serve()

	probes := startProbeWriter(ctx, dsnVia(t, sourceDSN, r.Addr()), probeAction)
	time.Sleep(1500 * time.Millisecond)
	if probes.writes.Load() == 0 {
		t.Fatal("no traffic reached the source before cutover")
	}

	err = Cutover(ctx, CutoverConfig{
		Router:        r,
		Progress:      &fwdProgress,
		SourceDSN:     sourceDSN,
		TargetDSN:     targetDSN,
		TargetBackend: hostPort(t, targetDSN),
		DrainTimeout:  30 * time.Second,
	})
	if err != nil {
		probes.Stop()
		t.Fatalf("cutover: %v", err)
	}
	writesAtCutover := probes.writes.Load()
	t.Logf("cut over to target after %d writes", writesAtCutover)

	// Replication is unidirectional per phase. Forward streaming stops here;
	// leaving it running would loop reverse-applied writes back to the target.
	stopForward()
	// The slot stays active until its walsender connection actually closes;
	// cancelling the context alone does not release it.
	fwdConn.Close(context.Background())
	if err := DropSlot(ctx, src.PgConn(), fwdSlot); err != nil {
		t.Fatalf("drop forward slot: %v", err)
	}

	// --- reverse pipeline ---
	revApply, err := pgx.Connect(ctx, sourceDSN)
	if err != nil {
		t.Fatalf("connect reverse applier: %v", err)
	}
	defer revApply.Close(context.Background())

	revCtx, stopReverse := context.WithCancel(ctx)
	defer stopReverse()
	var revProgress Progress
	targetMeta, err := LoadTables(ctx, dst)
	if err != nil {
		t.Fatalf("load target tables: %v", err)
	}
	go func() {
		_ = Stream(revCtx, revConn, revSlot, revPub, revStart, &revProgress, func(e WALEvent) error {
			m, ok := targetMeta[e.Schema+"."+e.Table]
			if !ok {
				return nil
			}
			return Apply(revCtx, revApply, m, e)
		})
	}()

	// Traffic now lands on the target and streams back to the source.
	time.Sleep(1500 * time.Millisecond)
	writesOnTarget := probes.writes.Load() - writesAtCutover
	if writesOnTarget == 0 {
		t.Fatal("no writes landed on the target during the rollback window")
	}
	t.Logf("%d writes landed while the target was authoritative", writesOnTarget)

	// --- the target "fails"; roll back inside the window ---
	start := time.Now()
	err = Rollback(ctx, RollbackConfig{
		Router:        r,
		Progress:      &revProgress,
		TargetDSN:     targetDSN,
		SourceDSN:     sourceDSN,
		SourceBackend: hostPort(t, sourceDSN),
		DrainTimeout:  30 * time.Second,
	})
	if err != nil {
		probes.Stop()
		t.Fatalf("rollback: %v", err)
	}
	writesAtRollback := probes.writes.Load()
	t.Logf("rolled back in %s after %d total writes", time.Since(start).Round(time.Millisecond), writesAtRollback)

	if r.Backend() != hostPort(t, sourceDSN) {
		t.Fatalf("router points at %s, not back at the source", r.Backend())
	}

	// Traffic must survive the second switch too.
	time.Sleep(1200 * time.Millisecond)
	probes.Stop()
	stopReverse()
	// As with the forward slot, the reverse slot is only released once its
	// walsender connection closes — teardown below would otherwise find it busy.
	revConn.Close(context.Background())

	if n := probes.failures.Load(); n != 0 {
		probes.mu.Lock()
		first := probes.firstErr
		probes.mu.Unlock()
		t.Errorf("%d client connections failed across cutover and rollback; first error: %v", n, first)
	}

	// The exit criterion: every write made while the target was authoritative
	// is back on the source, with nothing lost.
	var srcProbes int64
	if err := src.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE action = $1", probeAction).Scan(&srcProbes); err != nil {
		t.Fatal(err)
	}
	if srcProbes < writesAtRollback {
		t.Errorf("source holds %d probe writes but %d were acknowledged before rollback completed; "+
			"%d writes were lost", srcProbes, writesAtRollback, writesAtRollback-srcProbes)
	}

	// And writes keep flowing to the source afterwards.
	total := probes.writes.Load()
	if total <= writesAtRollback {
		t.Errorf("no writes landed after rollback: %d total vs %d at rollback", total, writesAtRollback)
	}
	if err := src.QueryRow(ctx, "SELECT count(*) FROM audit_log WHERE action = $1", probeAction).Scan(&srcProbes); err != nil {
		t.Fatal(err)
	}
	if srcProbes != total {
		t.Errorf("source holds %d probe writes, expected all %d", srcProbes, total)
	}

	// Point of no return, exercised: the reverse pipeline tears down cleanly.
	if err := TeardownReverse(ctx, targetDSN, revSlot, revPub); err != nil {
		t.Errorf("teardown reverse: %v", err)
	}
	var slots int
	if err := dst.QueryRow(ctx, "SELECT count(*) FROM pg_replication_slots WHERE slot_name = $1", revSlot).Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 0 {
		t.Error("reverse slot survived teardown; it would pin WAL on the target indefinitely")
	}
}
