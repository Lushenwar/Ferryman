// Command ferryman migrates a live Postgres database onto another one without
// taking the application offline.
//
// The engine package is a set of primitives whose ordering constraints are
// documented on each function; this is the caller that enforces them. Three of
// them are load-bearing and all three are enforced here by construction rather
// than by comment:
//
//   - The session that creates the replication slot must stay open for the
//     whole backfill, because postgres discards an exported snapshot when its
//     exporting session ends. The connection opened for the slot is the same one
//     the stream later runs on, so it cannot be closed early.
//   - No CDC event may be applied until the backfill has returned. Backfill is
//     called synchronously before the streaming goroutine starts.
//   - The reverse slot must exist before the target takes its first write, or a
//     rollback silently loses those writes. PrepareReverse runs before Cutover.
//
// Everything the two subcommands need is a DSN; there is no config file and no
// daemon. Cutover happens in this process because zero lag is proven by a marker
// the applier reports through an in-memory Progress, which a second process
// could not see.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"net"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"time"

	"github.com/Lushenwar/Ferryman/engine"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func main() {
	log.SetFlags(log.Ltime)
	if len(os.Args) < 2 {
		usage()
	}

	// Interrupt cancels the run. Slots are deliberately left in place: they are
	// what makes a restart resume instead of recopying.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var err error
	switch os.Args[1] {
	case "migrate":
		err = migrate(ctx, os.Args[2:])
	case "status":
		err = status(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("%s: %v", os.Args[1], err)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `ferryman — zero-downtime Postgres migration

  ferryman migrate  backfill, stream, verify, then switch traffic to the target
  ferryman status   report replication lag for a slot, from outside the migration

Run either with -h for flags.
`)
	os.Exit(2)
}

func migrate(ctx context.Context, argv []string) error {
	fs := flag.NewFlagSet("migrate", flag.ExitOnError)
	sourceDSN := fs.String("source", os.Getenv("FERRYMAN_SOURCE_DSN"), "source DSN (env FERRYMAN_SOURCE_DSN)")
	targetDSN := fs.String("target", os.Getenv("FERRYMAN_TARGET_DSN"), "target DSN (env FERRYMAN_TARGET_DSN)")
	listen := fs.String("listen", "127.0.0.1:6432", "address clients connect to instead of the database")
	slot := fs.String("slot", engine.DefaultSlot, "replication slot name")
	pub := fs.String("publication", engine.DefaultPublication, "publication name")
	parallelism := fs.Int("parallelism", 4, "backfill workers sharing the exported snapshot")
	maxLag := fs.Int64("max-lag", 8<<20, "replication lag, in bytes, the cutover will accept before it pauses traffic")
	drain := fs.Duration("drain-timeout", 30*time.Second, "budget for the paused part of the cutover")
	window := fs.Duration("window", time.Hour, "how long the reverse pipeline stays up after cutover")
	verify := fs.Bool("verify", true, "hash-compare every table while traffic is paused, and abort if they differ")
	noCutover := fs.Bool("no-cutover", false, "backfill and stream only, never switch traffic")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *sourceDSN == "" || *targetDSN == "" {
		return errors.New("-source and -target are required")
	}

	sourceBackend, err := hostPort(*sourceDSN)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	targetBackend, err := hostPort(*targetDSN)
	if err != nil {
		return fmt.Errorf("target: %w", err)
	}

	// One connection per side rather than a pool: Stream calls the applier
	// serially, so a pool would add a dependency and buy no concurrency. Both are
	// opened writable — the cutover flips the drained database to
	// default_transaction_read_only, and the replicated stream is the one writer
	// that still has to get through. Setting it as a connect parameter covers the
	// connection for its whole life.
	srcConn, err := connect(ctx, *sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer srcConn.Close(context.Background())

	dstConn, err := connect(ctx, *targetDSN)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer dstConn.Close(context.Background())

	// Lag polling gets its own connection. The two above belong to the applier
	// once the stream goroutine starts — it reads the source catalog to reconcile
	// schema changes — and a *pgx.Conn carries one statement at a time, so sharing
	// one would intermittently fail with "conn busy".
	monConn, err := connect(ctx, *sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source to monitor lag: %w", err)
	}
	defer monConn.Close(context.Background())

	tables, err := engine.LoadTables(ctx, srcConn)
	if err != nil {
		return err
	}
	list := tableList(tables)
	if len(list) == 0 {
		return errors.New("no user tables found on the source")
	}

	adminSrc, err := pgconn.Connect(ctx, *sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source for publication: %w", err)
	}
	if err := engine.EnsurePublication(ctx, adminSrc, *pub); err != nil {
		adminSrc.Close(context.Background())
		return err
	}
	adminSrc.Close(context.Background())

	// This connection holds the exported snapshot alive, so it lives until the
	// backfill has finished — and then becomes the stream, which is why it is not
	// closed in between.
	repl, err := engine.ReplicationConnect(ctx, *sourceDSN)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer repl.Close(context.Background())

	snapshot, consistent, err := engine.EnsureSlot(ctx, repl, *slot)
	if err != nil {
		return err
	}
	if snapshot == "" {
		// A slot that already exists has no retrievable snapshot, so there is no
		// consistent point to copy from; the target is assumed already backfilled
		// and the stream resumes from the slot's confirmed position.
		log.Printf("slot %s already exists: resuming the stream, skipping the backfill", *slot)
	} else {
		log.Printf("slot %s created at %s; backfilling %d tables with %d workers",
			*slot, consistent, len(list), *parallelism)
		start := time.Now()
		if err := engine.Backfill(ctx, *sourceDSN, *targetDSN, snapshot, list, *parallelism); err != nil {
			return fmt.Errorf("backfill: %w", err)
		}
		log.Printf("backfill complete in %s", time.Since(start).Round(time.Millisecond))
	}

	// Before the cutover, not after: the target must have a slot holding its WAL
	// before it takes its first write, or the rollback path has nothing to replay.
	reverseSlot, reversePub := *slot+"_reverse", *pub+"_reverse"
	var (
		reverseConn  *pgconn.PgConn
		reverseStart pglogrepl.LSN
	)
	if !*noCutover {
		reverseConn, reverseStart, err = engine.PrepareReverse(ctx, *targetDSN, reverseSlot, reversePub)
		if err != nil {
			return fmt.Errorf("prepare reverse pipeline: %w", err)
		}
		defer reverseConn.Close(context.Background())
	}

	router, err := engine.NewRouter(*listen, sourceBackend)
	if err != nil {
		return err
	}
	defer router.Close()
	go router.Serve()
	log.Printf("router listening on %s -> source %s", router.Addr(), sourceBackend)

	applier, err := engine.NewApplier(ctx, srcConn, dstConn)
	if err != nil {
		return err
	}
	var progress engine.Progress
	streamCtx, stopStream := context.WithCancel(ctx)
	defer stopStream()
	streamErr := make(chan error, 1)
	go func() {
		streamErr <- engine.Stream(streamCtx, repl, *slot, *pub, consistent, &progress, applier.Handle(streamCtx))
	}()

	if *noCutover {
		log.Print("streaming; -no-cutover is set so traffic stays on the source. ctrl-c to stop")
		return watchLag(ctx, monConn, *slot, streamErr)
	}

	// Wait for lag to come down *before* pausing traffic. Cutover pauses first and
	// then drains within DrainTimeout, so entering it with minutes of lag freezes
	// every write in the system only to abort seconds later.
	log.Printf("waiting for lag under %s before pausing traffic", byteCount(*maxLag))
	if err := waitLag(ctx, monConn, *slot, *maxLag, streamErr); err != nil {
		return err
	}

	cfg := engine.CutoverConfig{
		Router:        router,
		Progress:      &progress,
		SourceDSN:     *sourceDSN,
		TargetDSN:     *targetDSN,
		TargetBackend: targetBackend,
		DrainTimeout:  *drain,
	}
	if *verify {
		cfg.Verify = list
	}
	if err := engine.Cutover(ctx, cfg); err != nil {
		return fmt.Errorf("cutover aborted, traffic is still on the source: %w", err)
	}
	log.Printf("cutover complete: traffic now on target %s", targetBackend)

	stopStream()
	if err := <-streamErr; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("forward stream stopped: %v", err)
	}

	// The source is drained but still reachable, and a cron job or a client that
	// never learned about the cutover will happily write to it. Refusing those
	// writes outright is cheaper and more complete than reconciling them after.
	if err := engine.SetReadOnly(ctx, *sourceDSN, true); err != nil {
		log.Printf("warning: could not set the source read-only, stray writes there will conflict: %v", err)
	}

	reverse, err := engine.NewApplier(ctx, dstConn, srcConn)
	if err != nil {
		return err
	}
	if err := reverse.EnableConflictResolution(ctx); err != nil {
		// Without commit timestamps a replicated change cannot be ordered against
		// a local one, so it is replayed unconditionally. SetReadOnly above is what
		// keeps that from mattering.
		log.Printf("last-write-wins unavailable, replaying unconditionally: %v", err)
	}
	var reverseProgress engine.Progress
	reverseCtx, stopReverse := context.WithCancel(ctx)
	defer stopReverse()
	reverseErr := make(chan error, 1)
	go func() {
		reverseErr <- engine.Stream(reverseCtx, reverseConn, reverseSlot, reversePub,
			reverseStart, &reverseProgress, reverse.Handle(reverseCtx))
	}()

	log.Printf("reverse pipeline up; rollback window open for %s. press enter to roll back", *window)
	enter := make(chan struct{}, 1)
	go func() {
		if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err == nil {
			enter <- struct{}{}
		}
	}()

	select {
	case <-enter:
		log.Print("rolling back to the source")
		// Clear the guard first: the rollback syncs sequences onto the source and
		// then hands it live traffic, neither of which works read-only.
		if err := engine.SetReadOnly(ctx, *sourceDSN, false); err != nil {
			return fmt.Errorf("make the source writable again: %w", err)
		}
		if err := engine.Rollback(ctx, engine.RollbackConfig{
			Router:        router,
			Progress:      &reverseProgress,
			TargetDSN:     *targetDSN,
			SourceDSN:     *sourceDSN,
			SourceBackend: sourceBackend,
			DrainTimeout:  *drain,
		}); err != nil {
			return fmt.Errorf("rollback: %w", err)
		}
		stopReverse()
		<-reverseErr
		if err := engine.SetReadOnly(ctx, *targetDSN, true); err != nil {
			log.Printf("warning: could not set the target read-only: %v", err)
		}
		log.Printf("rolled back: traffic now on source %s, %d conflict(s) filed in %s",
			sourceBackend, reverse.Conflicts, engine.DLQTable)
		return nil

	case err := <-reverseErr:
		return fmt.Errorf("reverse stream failed inside the rollback window, rollback is no longer safe: %w", err)

	case <-time.After(*window):
		// Point of no return. The slot has to go: the target retains every WAL
		// segment it has not confirmed, until the disk fills.
		stopReverse()
		<-reverseErr
		log.Print("rollback window closed; tearing down the reverse pipeline")
		return engine.TeardownReverse(ctx, *targetDSN, reverseSlot, reversePub)

	case <-ctx.Done():
		return ctx.Err()
	}
}

// status reports lag without joining the migration, so it is safe to run from
// another shell while one is in progress.
func status(ctx context.Context, argv []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	dsn := fs.String("source", os.Getenv("FERRYMAN_SOURCE_DSN"), "DSN holding the slot (env FERRYMAN_SOURCE_DSN)")
	slot := fs.String("slot", engine.DefaultSlot, "replication slot name")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if *dsn == "" {
		return errors.New("-source is required")
	}

	conn, err := connect(ctx, *dsn)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	var active bool
	var behind int64
	err = conn.QueryRow(ctx, `
		SELECT active,
		       pg_wal_lsn_diff(pg_current_wal_lsn(), coalesce(confirmed_flush_lsn, restart_lsn))::bigint
		  FROM pg_replication_slots WHERE slot_name = $1`, *slot).Scan(&active, &behind)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("slot %s does not exist; no migration has been started", *slot)
	}
	if err != nil {
		return err
	}
	log.Printf("slot %s: active=%t, lag %s", *slot, active, byteCount(behind))
	return nil
}

// lag reports how much WAL the source has produced that the slot has not
// confirmed.
//
// It never reaches exactly zero, and is not supposed to: pg_current_wal_lsn()
// advances for checkpoints, vacuum and activity in other databases, none of
// which pgoutput delivers. So this is the right measure for "is lag small enough
// that pausing traffic is safe" and the wrong one for "has the stream caught
// up" — Cutover answers that with a marker it can watch arrive.
func lag(ctx context.Context, conn *pgx.Conn, slot string) (int64, error) {
	var behind int64
	err := conn.QueryRow(ctx, `
		SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), coalesce(confirmed_flush_lsn, restart_lsn))::bigint
		  FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&behind)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("slot %s disappeared", slot)
	}
	return behind, err
}

// waitLag blocks until lag is at or under limit, or the stream dies trying.
func waitLag(ctx context.Context, conn *pgx.Conn, slot string, limit int64, streamErr <-chan error) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		behind, err := lag(ctx, conn, slot)
		if err != nil {
			return err
		}
		if behind <= limit {
			log.Printf("lag %s, at or under the threshold", byteCount(behind))
			return nil
		}
		select {
		case err := <-streamErr:
			return fmt.Errorf("stream stopped while waiting for lag to fall: %w", err)
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// watchLag prints lag until the caller gives up. Only used by -no-cutover, which
// is the mode for watching a sync rather than completing one.
func watchLag(ctx context.Context, conn *pgx.Conn, slot string, streamErr <-chan error) error {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case err := <-streamErr:
			return err
		case <-ctx.Done():
			return nil
		case <-tick.C:
			behind, err := lag(ctx, conn, slot)
			if err != nil {
				return err
			}
			log.Printf("lag %s", byteCount(behind))
		}
	}
}

// connect opens a connection that stays writable even when the database has been
// flipped read-only by SetReadOnly. As a connect parameter this holds for the
// life of the connection, where a SET would only cover whichever connection ran
// it — the distinction that matters the moment a pool is used instead.
func connect(ctx context.Context, dsn string) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["default_transaction_read_only"] = "off"
	return pgx.ConnectConfig(ctx, cfg)
}

// hostPort extracts the address the router should proxy to. Parsing the DSN
// rather than asking for the address twice keeps them from disagreeing.
func hostPort(dsn string) (string, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return "", fmt.Errorf("parse dsn: %w", err)
	}
	return net.JoinHostPort(cfg.Host, strconv.Itoa(int(cfg.Port))), nil
}

// tableList orders the catalog map so backfill chunks and verify buckets are
// laid out the same way on every run, and drops the dead-letter table, which
// belongs to the migration rather than to the application.
func tableList(m map[string]engine.TableMeta) []engine.TableMeta {
	out := make([]engine.TableMeta, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if m[k].Table == engine.DLQTable {
			continue
		}
		out = append(out, m[k])
	}
	return out
}

func byteCount(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	v, exp := float64(n), 0
	for v >= unit && exp < 4 {
		v /= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", v, "KMGT"[exp-1])
}
