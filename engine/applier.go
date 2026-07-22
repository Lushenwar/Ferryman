package engine

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	switch e.Op {
	case "INSERT":
		return applyInsert(ctx, db, meta, e)
	case "UPDATE":
		return applyUpdate(ctx, db, meta, e)
	case "DELETE":
		return applyDelete(ctx, db, meta, e)
	default:
		return fmt.Errorf("unsupported op %q for %s", e.Op, meta.Qualified())
	}
}

// args accumulates query parameters and hands out their $n placeholders.
type args struct{ vals []any }

func (a *args) next(v any) string {
	a.vals = append(a.vals, v)
	return "$" + strconv.Itoa(len(a.vals))
}

// sortedCols gives statements a stable column order. Map iteration order would
// otherwise make the generated SQL non-deterministic and untestable.
func sortedCols(m map[string]any) []string {
	cols := make([]string, 0, len(m))
	for c := range m {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	return cols
}

func applyInsert(ctx context.Context, db Execer, meta TableMeta, e WALEvent) error {
	sql, vals, err := buildInsert(meta, e)
	if err != nil {
		return err
	}
	if _, err := db.Exec(ctx, sql, vals...); err != nil {
		return fmt.Errorf("insert into %s: %w", meta.Qualified(), err)
	}
	return nil
}

func buildInsert(meta TableMeta, e WALEvent) (string, []any, error) {
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

func applyUpdate(ctx context.Context, db Execer, meta TableMeta, e WALEvent) error {
	sql, vals, err := buildUpdate(meta, e)
	if err != nil {
		return err
	}
	if _, err := db.Exec(ctx, sql, vals...); err != nil {
		return fmt.Errorf("update %s: %w", meta.Qualified(), err)
	}
	return nil
}

func buildUpdate(meta TableMeta, e WALEvent) (string, []any, error) {
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
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		meta.Qualified(), strings.Join(sets, ", "), where), a.vals, nil
}

func applyDelete(ctx context.Context, db Execer, meta TableMeta, e WALEvent) error {
	sql, vals, err := buildDelete(meta, e)
	if err != nil {
		return err
	}
	if _, err := db.Exec(ctx, sql, vals...); err != nil {
		return fmt.Errorf("delete from %s: %w", meta.Qualified(), err)
	}
	return nil
}

func buildDelete(meta TableMeta, e WALEvent) (string, []any, error) {
	var a args
	where, err := keyPredicate(meta, identity(e), &a)
	if err != nil {
		return "", nil, err
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
