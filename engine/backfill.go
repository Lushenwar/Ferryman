package engine

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/sync/errgroup"
)

// Backfill bulk-copies every table from an exported snapshot into the target.
//
// The snapshot name comes from EnsureSlot. It pins the source to the exact
// state at the slot's consistent point, so the copy and the WAL stream meet
// with no gap and no overlap: anything committed before that LSN is in the
// snapshot, anything after it is in the stream.
//
// Two things the caller must get right:
//
//   - The connection that created the slot has to stay open for the whole
//     backfill. Postgres discards an exported snapshot when its exporting
//     session ends, and every worker here binds to it by name.
//   - CDC events must not be applied to the target until this returns. An
//     UPDATE whose row has not been copied yet would apply to nothing and then
//     be overwritten by the older snapshot row. See the note on Apply.
//
// Tables are copied in parallel; several sessions can share one exported
// snapshot, which is most of the reason to export it in the first place.
func Backfill(ctx context.Context, sourceDSN, targetDSN, snapshot string, tables []TableMeta, parallelism int) error {
	if snapshot == "" {
		return fmt.Errorf("no exported snapshot: the slot already existed, so there is no consistent point to copy from")
	}
	if parallelism < 1 {
		parallelism = 1
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)
	for _, meta := range tables {
		g.Go(func() error {
			if err := backfillTable(gctx, sourceDSN, targetDSN, snapshot, meta); err != nil {
				return fmt.Errorf("backfill %s: %w", meta.Qualified(), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return SyncSequences(ctx, sourceDSN, targetDSN)
}

// SyncSequences copies every sequence's position from source to target.
//
// The copy writes explicit key values, so the target's sequences are left
// wherever they started — usually at 1. The next insert generated on the target
// would then collide with an already-copied row. Cutover runs this again once
// the stream has drained, to pick up values CDC advanced past.
func SyncSequences(ctx context.Context, sourceDSN, targetDSN string) error {
	src, err := pgconn.Connect(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source for sequences: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := pgconn.Connect(ctx, targetDSN)
	if err != nil {
		return fmt.Errorf("connect target for sequences: %w", err)
	}
	defer dst.Close(context.Background())

	// last_value is NULL until the sequence is first used; is_called must then
	// be false so the very next nextval() returns the start value rather than
	// skipping it.
	res, err := src.Exec(ctx, `
		SELECT quote_ident(schemaname) || '.' || quote_ident(sequencename),
		       coalesce(last_value, start_value)::text,
		       (last_value IS NOT NULL)::text
		  FROM pg_sequences`).ReadAll()
	if err != nil {
		return fmt.Errorf("read source sequences: %w", err)
	}

	for _, r := range res {
		for _, row := range r.Rows {
			name, value, isCalled := string(row[0]), string(row[1]), string(row[2])
			stmt := fmt.Sprintf("SELECT setval(%s, %s, %s)",
				quoteLiteral(name), value, isCalled)
			if err := dst.Exec(ctx, stmt).Close(); err != nil {
				return fmt.Errorf("setval %s: %w", name, err)
			}
		}
	}
	return nil
}

func backfillTable(ctx context.Context, sourceDSN, targetDSN, snapshot string, meta TableMeta) error {
	src, err := pgconn.Connect(ctx, sourceDSN)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer src.Close(context.Background())

	dst, err := pgconn.Connect(ctx, targetDSN)
	if err != nil {
		return fmt.Errorf("connect target: %w", err)
	}
	defer dst.Close(context.Background())

	// REPEATABLE READ is required before SET TRANSACTION SNAPSHOT, and the SET
	// must come before any query in the transaction.
	begin := "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY; " +
		"SET TRANSACTION SNAPSHOT " + quoteLiteral(snapshot)
	if err := src.Exec(ctx, begin).Close(); err != nil {
		return fmt.Errorf("enter snapshot %s: %w", snapshot, err)
	}
	defer func() { _ = src.Exec(context.Background(), "COMMIT").Close() }()

	cols := make([]string, len(meta.Columns))
	for i, c := range meta.Columns {
		cols[i] = quoteIdent(c.Name)
	}
	list := strings.Join(cols, ", ")

	// Stream the copy rather than buffering it: a table larger than memory
	// should still migrate.
	pr, pw := io.Pipe()
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		_, err := src.CopyTo(ctx, pw,
			fmt.Sprintf("COPY (SELECT %s FROM %s) TO STDOUT", list, meta.Qualified()))
		// Closing with the error propagates a source failure to the reader
		// instead of letting the target see a silently truncated stream.
		pw.CloseWithError(err)
		return err
	})
	g.Go(func() error {
		_, err := dst.CopyFrom(ctx, pr,
			fmt.Sprintf("COPY %s (%s) FROM STDIN", meta.Qualified(), list))
		pr.CloseWithError(err)
		return err
	})
	return g.Wait()
}
