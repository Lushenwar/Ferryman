package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Apply writes one decoded WAL event to the target.
//
// Every statement it builds is idempotent, because the stream redelivers a
// whole transaction after any interrupted apply:
//
//	INSERT  upserts, so a replayed insert overwrites instead of colliding
//	UPDATE  writes the same values again to the same key, which is a no-op
//	DELETE  removing an already-removed row affects zero rows, which is fine
//
// UPDATE deliberately does not upsert. Its new tuple can be incomplete —
// unchanged TOAST columns are withheld — so a row missing from the target
// cannot be reconstructed from the event alone. The pipeline instead
// guarantees the row is already there: the backfill reads the snapshot
// exported by the replication slot, so every row an UPDATE can refer to
// existed at the slot's LSN and is therefore in the snapshot. Phase 3 must
// preserve that ordering.
func Apply(ctx context.Context, db Execer, meta TableMeta, e WALEvent) error {
	_, err := apply(ctx, db, meta, e, time.Time{})
	return err
}

// apply reports how many rows the statement touched, which is how a caller
// running last-write-wins learns that its write lost.
//
// lww is the incoming change's commit time, or the zero time to apply
// unconditionally. See lastWriteWins.
func apply(ctx context.Context, db Execer, meta TableMeta, e WALEvent, lww time.Time) (int64, error) {
	var (
		sql  string
		vals []any
		err  error
	)
	switch e.Op {
	case "INSERT":
		sql, vals, err = buildInsert(meta, e, lww)
	case "UPDATE":
		sql, vals, err = buildUpdate(meta, e, lww)
	case "DELETE":
		sql, vals, err = buildDelete(meta, e, lww)
	default:
		return 0, fmt.Errorf("unsupported op %q for %s", e.Op, meta.Qualified())
	}
	if err != nil {
		return 0, err
	}
	tag, err := db.Exec(ctx, sql, vals...)
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", strings.ToLower(e.Op), meta.Qualified(), err)
	}
	return tag.RowsAffected(), nil
}

// lastWriteWins renders the guard that keeps a replicated change from
// overwriting a row the receiving database wrote more recently.
//
// row is how the existing row is addressed in the statement: bare in an UPDATE
// or DELETE, table-qualified inside ON CONFLICT DO UPDATE, where an unqualified
// xmin would be ambiguous.
//
// pg_xact_commit_timestamp returns NULL when track_commit_timestamp is off, and
// for rows old enough to have been frozen. Both are coalesced to -infinity so
// the replicated write proceeds: skipping it would silently drop data on the
// strength of an answer we did not get.
func lastWriteWins(row string, a *args, at time.Time) string {
	return fmt.Sprintf("coalesce(pg_xact_commit_timestamp(%sxmin), '-infinity') < CAST(%s AS timestamptz)",
		row, a.next(at))
}

// args accumulates query parameters and hands out their $n placeholders.
type args struct{ vals []any }

func (a *args) next(v any) string {
	a.vals = append(a.vals, v)
	return "$" + strconv.Itoa(len(a.vals))
}

// sortedCols gives statements a stable column order. Map iteration order would
// otherwise make the generated SQL non-deterministic and untestable.
func sortedCols(m map[string]any) []string { return slices.Sorted(maps.Keys(m)) }

func buildInsert(meta TableMeta, e WALEvent, lww time.Time) (string, []any, error) {
	cols := sortedCols(e.Data)
	if len(cols) == 0 {
		return "", nil, fmt.Errorf("insert into %s has no columns", meta.Qualified())
	}

	var a args
	names := make([]string, len(cols))
	placeholders := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteIdent(c)
		placeholders[i] = a.next(e.Data[c])
	}

	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO %s (%s) VALUES (%s)",
		meta.Qualified(), strings.Join(names, ", "), strings.Join(placeholders, ", "))

	if meta.HasPK() {
		// Replaying an insert must overwrite, not fail. Only the columns this
		// event actually carries are refreshed, so a withheld TOAST value on
		// the target survives the upsert.
		sets := make([]string, 0, len(cols))
		for _, c := range cols {
			if isKey(meta, c) {
				continue
			}
			sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", quoteIdent(c), quoteIdent(c)))
		}
		keys := make([]string, len(meta.KeyColumns))
		for i, k := range meta.KeyColumns {
			keys[i] = quoteIdent(k)
		}
		if len(sets) == 0 {
			// Table is nothing but its key; there is no non-key column to refresh.
			fmt.Fprintf(&b, " ON CONFLICT (%s) DO NOTHING", strings.Join(keys, ", "))
		} else {
			fmt.Fprintf(&b, " ON CONFLICT (%s) DO UPDATE SET %s",
				strings.Join(keys, ", "), strings.Join(sets, ", "))
			if !lww.IsZero() {
				fmt.Fprintf(&b, " WHERE %s", lastWriteWins(quoteIdent(meta.Table)+".", &a, lww))
			}
		}
		return b.String(), a.vals, nil
	}

	// No primary key means no ON CONFLICT target. Guard the insert with a
	// whole-row existence check instead so a replay does not duplicate the row.
	where, err := keyPredicate(meta, e.Data, &a)
	if err != nil {
		return "", nil, err
	}
	sql := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s WHERE NOT EXISTS (SELECT 1 FROM %s WHERE %s)",
		meta.Qualified(), strings.Join(names, ", "), strings.Join(placeholders, ", "),
		meta.Qualified(), where)
	return sql, a.vals, nil
}

func buildUpdate(meta TableMeta, e WALEvent, lww time.Time) (string, []any, error) {
	var a args
	var sets []string
	for _, c := range sortedCols(e.Data) {
		// An unchanged TOAST column is never in Data — the decoder drops it —
		// but check anyway so a hand-built event cannot corrupt the target by
		// writing NULL over a large value (danger zone #2).
		if e.IsToast[c] {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = %s", quoteIdent(c), a.next(e.Data[c])))
	}
	if len(sets) == 0 {
		return "", nil, fmt.Errorf("update %s has nothing to set", meta.Qualified())
	}

	// Key off the pre-image: the update may have changed the key itself, in
	// which case the new values point at a row that does not exist yet.
	where, err := keyPredicate(meta, identity(e), &a)
	if err != nil {
		return "", nil, err
	}
	if !lww.IsZero() {
		where += " AND " + lastWriteWins("", &a, lww)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		meta.Qualified(), strings.Join(sets, ", "), where), a.vals, nil
}

func buildDelete(meta TableMeta, e WALEvent, lww time.Time) (string, []any, error) {
	var a args
	where, err := keyPredicate(meta, identity(e), &a)
	if err != nil {
		return "", nil, err
	}
	if !lww.IsZero() {
		where += " AND " + lastWriteWins("", &a, lww)
	}
	return fmt.Sprintf("DELETE FROM %s WHERE %s", meta.Qualified(), where), a.vals, nil
}

// identity picks the row image that identifies the row: the pre-image when the
// event has one, else the new values.
func identity(e WALEvent) map[string]any {
	if len(e.OldData) > 0 {
		return e.OldData
	}
	return e.Data
}

// keyPredicate builds the WHERE clause matching one row.
//
// It compares with IS NOT DISTINCT FROM rather than =, because a PK-less table
// keys on all its columns and any of those may be NULL, where = yields NULL and
// matches nothing. That operator gives no type inference for a bare parameter,
// so each one is cast to its catalog type.
func keyPredicate(meta TableMeta, row map[string]any, a *args) (string, error) {
	if len(meta.KeyColumns) == 0 {
		return "", fmt.Errorf("%s has no key columns", meta.Qualified())
	}
	preds := make([]string, 0, len(meta.KeyColumns))
	for _, k := range meta.KeyColumns {
		col, ok := meta.column(k)
		if !ok {
			return "", fmt.Errorf("key column %s missing from %s metadata", k, meta.Qualified())
		}
		val, present := row[k]
		if !present {
			// A withheld TOAST value in a key column leaves the row
			// unidentifiable; applying anyway would hit the wrong rows.
			return "", fmt.Errorf("key column %s of %s absent from event; "+
				"table needs REPLICA IDENTITY FULL or a smaller key", k, meta.Qualified())
		}
		preds = append(preds, fmt.Sprintf("%s IS NOT DISTINCT FROM CAST(%s AS %s)",
			quoteIdent(k), a.next(val), col.Type))
	}
	return strings.Join(preds, " AND "), nil
}

func isKey(meta TableMeta, col string) bool {
	for _, k := range meta.KeyColumns {
		if k == col {
			return true
		}
	}
	return false
}
