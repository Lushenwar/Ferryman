package engine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// PrepareReverse sets up Target -> Source replication and returns the walsender
// connection to stream it from, plus the LSN to start at.
//
// Call this *before* Cutover, not after. The slot must exist before the target
// takes its first write, or those writes produce no WAL anyone is holding and
// the rollback path silently loses them. Creating it early is free: until
// cutover the target receives no traffic, so the slot simply sits idle.
//
// The returned connection is the caller's to close.
func PrepareReverse(ctx context.Context, targetDSN, slot, publication string) (*pgconn.PgConn, pglogrepl.LSN, error) {
	admin, err := pgconn.Connect(ctx, targetDSN)
	if err != nil {
		return nil, 0, fmt.Errorf("connect target to prepare reverse: %w", err)
	}
	defer admin.Close(context.Background())

	if err := EnsurePublication(ctx, admin, publication); err != nil {
		return nil, 0, err
	}

	conn, err := ReplicationConnect(ctx, targetDSN)
	if err != nil {
		return nil, 0, fmt.Errorf("reverse replication connect: %w", err)
	}
	// No snapshot is needed here, unlike the forward direction: the source is
	// already an exact copy as of the cutover, so there is nothing to backfill.
	_, consistent, err := EnsureSlot(ctx, conn, slot)
	if err != nil {
		conn.Close(context.Background())
		return nil, 0, err
	}
	// A pre-existing slot reports no consistent point; zero tells Stream to
	// resume from the slot's own confirmed position, which is what we want.
	return conn, consistent, nil
}

// RollbackConfig describes returning traffic to the source inside the rollback
// window.
type RollbackConfig struct {
	Router   *Router
	Progress *Progress // progress of the reverse (Target -> Source) stream

	TargetDSN string // the database currently serving traffic
	SourceDSN string // the database being returned to

	SourceBackend string
	DrainTimeout  time.Duration
}

// Rollback returns traffic to the source with no lost writes.
//
// It is the cutover run in the other direction, and deliberately shares that
// code: pause traffic, drain the reverse stream to zero lag, sync sequences,
// repoint, resume. A separate implementation would be a second place for the
// ordering to be got wrong, on the path that only ever runs when something has
// already gone wrong.
func Rollback(ctx context.Context, cfg RollbackConfig) error {
	return Cutover(ctx, CutoverConfig{
		Router:   cfg.Router,
		Progress: cfg.Progress,
		// The live database is the one the marker is emitted on and the one
		// sequences are read from — here that is the target.
		SourceDSN:     cfg.TargetDSN,
		TargetDSN:     cfg.SourceDSN,
		TargetBackend: cfg.SourceBackend,
		DrainTimeout:  cfg.DrainTimeout,
	})
}

// TeardownReverse is the point of no return: once the rollback window closes,
// the reverse pipeline is dismantled and the target becomes permanently
// authoritative. Leaving the slot in place instead would pin WAL on the target
// forever (danger zone #4).
func TeardownReverse(ctx context.Context, targetDSN, slot, publication string) error {
	conn, err := pgconn.Connect(ctx, targetDSN)
	if err != nil {
		return fmt.Errorf("connect target to tear down reverse: %w", err)
	}
	defer conn.Close(context.Background())

	if err := DropSlot(ctx, conn, slot); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+quoteIdent(publication)).Close(); err != nil {
		return fmt.Errorf("drop publication %s: %w", publication, err)
	}
	return nil
}

// DropSlot removes a replication slot if it exists. An orphaned slot is not
// harmless: postgres retains every WAL segment the slot has not confirmed,
// until the disk fills.
//
// Closing the streaming connection releases the slot asynchronously, so a drop
// issued right after a stream stops can still find it active. That is transient
// and worth waiting out; any other failure is returned immediately.
func DropSlot(ctx context.Context, conn *pgconn.PgConn, slot string) error {
	const stmt = "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = "

	deadline := time.Now().Add(10 * time.Second)
	for {
		err := conn.Exec(ctx, stmt+quoteLiteral(slot)).Close()
		if err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55006" || time.Now().After(deadline) {
			return fmt.Errorf("drop slot %s: %w", slot, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
