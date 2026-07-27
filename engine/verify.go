package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"golang.org/x/sync/errgroup"
)

// DefaultVerifyBuckets is how finely Verify partitions a table. It buys
// localisation, not speed: every bucket is hashed by one grouped query, so the
// count costs nothing extra and only decides how small a slice a mismatch is
// narrowed down to.
const DefaultVerifyBuckets = 64

// Mismatch is one bucket whose contents differ between source and target.
type Mismatch struct {
	Table  string
	Bucket int
	Filter string // predicate selecting this bucket, to drill into with a plain query

	SourceRows, TargetRows int64
	SourceHash, TargetHash string
}

func (m Mismatch) String() string {
	return fmt.Sprintf("%s bucket %d: source %d rows/%s, target %d rows/%s (drill in with: SELECT * FROM %s WHERE %s)",
		m.Table, m.Bucket, m.SourceRows, m.SourceHash, m.TargetRows, m.TargetHash, m.Table, m.Filter)
}

// Verify compares source and target row by row, and reports where they differ.
//
// It hashes each row and folds those hashes into buckets, so what crosses the
// network is one hash per bucket rather than the table. That is a Merkle tree
// one level deep, which is as deep as it is worth building: a single grouped
// query returns every bucket on a side, so the bisection an interior node would
// enable is saving a round trip that is not being made. A bucket that differs
// carries the predicate to select it, and the rows themselves are one ordinary
// query away.
//
// Buckets are logical — md5 of the row's key columns — deliberately, and not
// phase 7's ctid page ranges. Those partition the source's heap; the target
// holds the same rows in an entirely different physical order, so equal ctid
// bounds would compare unrelated sets of rows. Hashing the key instead means
// both sides agree on which bucket a row belongs to, and an edited row stays
// where it was, so the difference localises instead of smearing across two
// buckets. md5 rather than the cheaper hashtext because a migration between two
// Postgres major versions still has to agree on the partition.
//
// The two databases must be quiescent, or lag alone shows up as divergence.
// Cutover is the moment that holds: after the drain, before the switch.
func Verify(ctx context.Context, sourceDSN, targetDSN string, tables []TableMeta, buckets int) ([]Mismatch, error) {
	if buckets < 1 {
		buckets = DefaultVerifyBuckets
	}

	var source, target map[string][]bucketHash
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) {
		source, err = hashSide(gctx, sourceDSN, tables, buckets)
		return err
	})
	g.Go(func() (err error) {
		target, err = hashSide(gctx, targetDSN, tables, buckets)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	var out []Mismatch
	for _, meta := range tables {
		name := meta.Qualified()
		src, dst := index(source[name]), index(target[name])
		for b := 0; b < buckets; b++ {
			s, d := src[b], dst[b]
			if s == d {
				continue
			}
			out = append(out, Mismatch{
				Table:      name,
				Bucket:     b,
				Filter:     bucketExpr(meta, buckets) + " = " + fmt.Sprint(b),
				SourceRows: s.rows, TargetRows: d.rows,
				SourceHash: s.hash, TargetHash: d.hash,
			})
		}
	}
	return out, nil
}

type bucketHash struct {
	bucket int
	rows   int64
	hash   string
}

// comparable payload only; the bucket number is the map key.
type sideBucket struct {
	rows int64
	hash string
}

func index(hs []bucketHash) map[int]sideBucket {
	m := make(map[int]sideBucket, len(hs))
	for _, h := range hs {
		m[h.bucket] = sideBucket{rows: h.rows, hash: h.hash}
	}
	return m
}

// bucketExpr assigns a row to a bucket from its key columns.
//
// Seven hex digits make 28 bits, which is always non-negative — taking 32 would
// let the value be INT_MIN, where the modulo is negative and abs() overflows.
func bucketExpr(meta TableMeta, buckets int) string {
	cols := make([]string, len(meta.KeyColumns))
	for i, k := range meta.KeyColumns {
		cols[i] = quoteIdent(k)
	}
	return fmt.Sprintf("(('x' || left(md5(ROW(%s)::text), 7))::bit(28)::int %% %d)",
		strings.Join(cols, ", "), buckets)
}

func hashSide(ctx context.Context, dsn string, tables []TableMeta, buckets int) (map[string][]bucketHash, error) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect %s to verify: %w", redactDSN(dsn), err)
	}
	defer conn.Close(context.Background())

	// Rows are hashed through their text rendering, and these three settings
	// change that rendering without changing the data. Left to the two clusters'
	// own defaults, identical rows would hash differently.
	if _, err := conn.Exec(ctx, "SET TimeZone = 'UTC'; SET DateStyle = 'ISO, YMD'; SET extra_float_digits = 3"); err != nil {
		return nil, fmt.Errorf("pin output formatting: %w", err)
	}

	out := map[string][]bucketHash{}
	for _, meta := range tables {
		cols := make([]string, len(meta.Columns))
		for i, c := range meta.Columns {
			cols[i] = quoteIdent(c.Name)
		}
		// Ordering by the row hash rather than by key: the aggregate has to be
		// insensitive to the order rows are stored in, which differs between a
		// copied heap and one built by upserts.
		q := fmt.Sprintf(`
			SELECT bucket, count(*), md5(string_agg(h, '' ORDER BY h))
			  FROM (SELECT %s AS bucket, md5(ROW(%s)::text) AS h FROM %s) s
			 GROUP BY bucket`,
			bucketExpr(meta, buckets), strings.Join(cols, ", "), meta.Qualified())

		rows, err := conn.Query(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("hash %s: %w", meta.Qualified(), err)
		}
		var hs []bucketHash
		for rows.Next() {
			var h bucketHash
			if err := rows.Scan(&h.bucket, &h.rows, &h.hash); err != nil {
				rows.Close()
				return nil, err
			}
			hs = append(hs, h)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("hash %s: %w", meta.Qualified(), err)
		}
		out[meta.Qualified()] = hs
	}
	return out, nil
}

// redactDSN keeps a connection string's password out of error messages.
func redactDSN(dsn string) string {
	if at := strings.LastIndex(dsn, "@"); at >= 0 {
		if slash := strings.Index(dsn, "//"); slash >= 0 && slash+2 < at {
			return dsn[:slash+2] + "***" + dsn[at:]
		}
	}
	return dsn
}
