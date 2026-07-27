package engine

import (
	"context"
	"fmt"
	"io"
	"strconv"
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
// Work is split into chunks of heap pages, not one unit per table, and several
// sessions share the one exported snapshot — which is most of the reason to
// export it in the first place. Splitting per table alone leaves N-1 workers
// idle while the largest table finishes on a single connection, and the largest
// table is the one that decides how long a migration takes.
func Backfill(ctx context.Context, sourceDSN, targetDSN, snapshot string, tables []TableMeta, parallelism int) error {
	if snapshot == "" {
		return fmt.Errorf("no exported snapshot: the slot already existed, so there is no consistent point to copy from")
	}
	if parallelism < 1 {
		parallelism = 1
	}

	chunks, err := planChunks(ctx, sourceDSN, tables, parallelism)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(parallelism)
	for _, c := range chunks {
		g.Go(func() error {
			if err := copyChunk(gctx, sourceDSN, targetDSN, snapshot, c); err != nil {
				return fmt.Errorf("backfill %s%s: %w", c.meta.Qualified(), c.describe(), err)
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return SyncSequences(ctx, sourceDSN, targetDSN)
}

// chunk is one contiguous range of a table's heap, addressed by ctid.
//
// Slicing on physical pages rather than primary key ranges means every table
// qualifies whatever its key looks like — a composite key, or none at all —
// with no min/max probe, and the pieces are balanced by the thing that actually
// governs copy time, which is pages read. Key ranges balance by key
// distribution, which on a sparse or clustered key is not the same thing.
//
// ctid is stable for the duration: the snapshot transactions hold ACCESS SHARE,
// which blocks the VACUUM FULL and CLUSTER that would move rows between pages.
type chunk struct {
	meta      TableMeta
	lo        uint32 // first heap page, inclusive
	hi        uint32 // last heap page, exclusive; meaningless when unbounded
	unbounded bool
}

// where restricts the copy to this chunk's pages.
//
// The final chunk is deliberately left open-ended. relpages is an estimate
// maintained by VACUUM and ANALYZE, so the heap may well have grown past it,
// and an upper bound on the last chunk would drop those rows silently — the one
// failure mode a backfill must not have.
func (c chunk) where() string {
	if c.lo == 0 && c.unbounded {
		return ""
	}
	// ponytail: postgres 14+ answers these with a TID range scan. On older
	// versions it is a sequential scan with a filter — correct, just not faster.
	pred := fmt.Sprintf(" WHERE ctid >= '(%d,0)'::tid", c.lo)
	if !c.unbounded {
		pred += fmt.Sprintf(" AND ctid < '(%d,0)'::tid", c.hi)
	}
	return pred
}

func (c chunk) describe() string {
	if c.lo == 0 && c.unbounded {
		return ""
	}
	if c.unbounded {
		return fmt.Sprintf(" pages %d..end", c.lo)
	}
	return fmt.Sprintf(" pages %d..%d", c.lo, c.hi)
}

// ponytail: a var rather than a const purely so the test can force a split on a
// fixture far smaller than any table this exists for.
var minChunkPages uint32 = 1024

// pageBounds returns the first page of each chunk, so chunk i covers
// [bounds[i], bounds[i+1]) and the last one runs to the end of the heap.
//
// A table is left whole unless it is big enough that the split pays for the
// extra connection and snapshot per worker.
func pageBounds(pages uint32, parallelism int) []uint32 {
	n := parallelism
	if limit := int(pages / minChunkPages); n > limit {
		n = limit
	}
	if n < 1 {
		n = 1
	}
	per := pages / uint32(n)
	bounds := make([]uint32, n)
	for i := range bounds {
		bounds[i] = uint32(i) * per
	}
	return bounds
}

// planChunks sizes every table and lays out the work before any copying starts,
// so one flat pool of workers drains the whole plan. Splitting per table and
// then again within each table would oversubscribe by parallelism squared.
func planChunks(ctx context.Context, sourceDSN string, tables []TableMeta, parallelism int) ([]chunk, error) {
	conn, err := pgconn.Connect(ctx, sourceDSN)
	if err != nil {
		return nil, fmt.Errorf("connect source to plan backfill: %w", err)
	}
	defer conn.Close(context.Background())

	var out []chunk
	for _, meta := range tables {
		pages, err := relPages(ctx, conn, meta)
		if err != nil {
			return nil, err
		}
		bounds := pageBounds(pages, parallelism)
		for i, lo := range bounds {
			c := chunk{meta: meta, lo: lo, unbounded: i == len(bounds)-1}
			if !c.unbounded {
				c.hi = bounds[i+1]
			}
			out = append(out, c)
		}
	}
	return out, nil
}

// relPages reports the planner's page count for a table. It is an estimate, and
// zero for a table never vacuumed or analysed — both of which are fine, because
// an undercount only means fewer chunks and the last chunk is unbounded.
func relPages(ctx context.Context, conn *pgconn.PgConn, meta TableMeta) (uint32, error) {
	res, err := conn.Exec(ctx, "SELECT relpages FROM pg_class WHERE oid = "+
		quoteLiteral(meta.Qualified())+"::regclass").ReadAll()
	if err != nil {
		return 0, fmt.Errorf("size %s: %w", meta.Qualified(), err)
	}
	if len(res) == 0 || len(res[0].Rows) == 0 {
		return 0, fmt.Errorf("size %s: table not found on the source", meta.Qualified())
	}
	n, err := strconv.ParseUint(string(res[0].Rows[0][0]), 10, 32)
	if err != nil {
		return 0, fmt.Errorf("size %s: %w", meta.Qualified(), err)
	}
	return uint32(n), nil
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

func copyChunk(ctx context.Context, sourceDSN, targetDSN, snapshot string, c chunk) error {
	meta := c.meta

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
			fmt.Sprintf("COPY (SELECT %s FROM %s%s) TO STDOUT", list, meta.Qualified(), c.where()))
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
