package engine

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// DB is a handle that can both read the catalog and write to it. *pgxpool.Pool,
// *pgx.Conn and pgx.Tx all satisfy it.
type DB interface {
	RowQuerier
	Execer
}

// colDef is one column as the catalog describes it, in enough detail to
// recreate it on the other side.
type colDef struct {
	Type    string // format_type output, e.g. "text", "numeric(10,2)"
	NotNull bool
	Default *string // nil when the column has none
}

type tableSchema struct {
	Qualified string // schema-qualified and quoted, ready to paste into DDL
	Cols      map[string]colDef
}

const schemaQuery = `
SELECT n.nspname,
       c.relname,
       a.attname,
       format_type(a.atttypid, a.atttypmod),
       a.attnotnull,
       pg_get_expr(d.adbin, d.adrelid)
  FROM pg_attribute a
  JOIN pg_class     c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
 WHERE c.relkind = 'r'
   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
   AND a.attnum > 0
   AND NOT a.attisdropped
 ORDER BY n.nspname, c.relname, a.attnum`

func readSchema(ctx context.Context, q RowQuerier) (map[string]tableSchema, error) {
	rows, err := q.Query(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("read schema: %w", err)
	}
	defer rows.Close()

	out := map[string]tableSchema{}
	for rows.Next() {
		var schema, table, col string
		var def colDef
		if err := rows.Scan(&schema, &table, &col, &def.Type, &def.NotNull, &def.Default); err != nil {
			return nil, err
		}
		key := schema + "." + table
		t, ok := out[key]
		if !ok {
			t = tableSchema{
				Qualified: quoteIdent(schema) + "." + quoteIdent(table),
				Cols:      map[string]colDef{},
			}
			out[key] = t
		}
		t.Cols[col] = def
	}
	return out, rows.Err()
}

// SyncSchema reconciles the target's columns with the source's and returns the
// statements it ran.
//
// pgoutput streams DML only, so an ALTER TABLE on the source reaches us as a
// row change carrying a column the target does not have. That is enough of a
// signal: it arrives in commit order, immediately before the first DML that
// needs the new column, which is precisely when the target must be widened.
// Reading the two catalogs here is therefore both simpler and better ordered
// than capturing DDL with source-side event triggers and shipping it out of
// band.
//
// Two deliberate limits:
//
//   - ponytail: additive only. A table present on the source but not on the
//     target is skipped rather than created, because a faithful CREATE TABLE
//     means indexes, constraints, identity columns and their sequences. The
//     applier fails loudly on such a table instead of guessing.
//   - A column dropped on the source is not dropped on the target — that
//     destroys data on the word of a schema diff. Its NOT NULL is dropped
//     instead, which is what would otherwise stall every subsequent insert.
func SyncSchema(ctx context.Context, source RowQuerier, target DB) ([]string, error) {
	src, err := readSchema(ctx, source)
	if err != nil {
		return nil, err
	}
	dst, err := readSchema(ctx, target)
	if err != nil {
		return nil, err
	}

	var applied []string
	for _, key := range slices.Sorted(maps.Keys(src)) {
		have, ok := dst[key]
		if !ok {
			continue
		}
		want := src[key]

		for _, name := range slices.Sorted(maps.Keys(want.Cols)) {
			if _, exists := have.Cols[name]; exists {
				continue
			}
			c := want.Cols[name]
			var b strings.Builder
			fmt.Fprintf(&b, "ALTER TABLE %s ADD COLUMN %s %s", have.Qualified, quoteIdent(name), c.Type)
			// Default before NOT NULL: without one, adding a NOT NULL column to a
			// table that already has rows is rejected outright.
			if c.Default != nil {
				b.WriteString(" DEFAULT " + *c.Default)
			}
			if c.NotNull {
				b.WriteString(" NOT NULL")
			}
			stmt := b.String()
			if _, err := target.Exec(ctx, stmt); err != nil {
				return applied, fmt.Errorf("%s: %w", stmt, err)
			}
			applied = append(applied, stmt)
		}

		for _, name := range slices.Sorted(maps.Keys(have.Cols)) {
			if _, exists := want.Cols[name]; exists {
				continue
			}
			c := have.Cols[name]
			if !c.NotNull || c.Default != nil {
				continue // inserts that omit it will still succeed
			}
			stmt := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL",
				have.Qualified, quoteIdent(name))
			if _, err := target.Exec(ctx, stmt); err != nil {
				return applied, fmt.Errorf("%s: %w", stmt, err)
			}
			applied = append(applied, stmt)
		}
	}
	return applied, nil
}

// Applier applies decoded WAL events to the target, reconciling the target's
// schema on the fly when the source changes shape mid-stream.
//
// Its handle is the one Stream wants. Give it a pool or a plain connection, not
// a transaction: recovering from a schema change means running DDL and retrying
// after a failed statement, and inside a transaction that statement has already
// aborted everything.
type Applier struct {
	Target DB
	Source RowQuerier

	// LastWriteWins keeps a replicated change from overwriting a row the target
	// wrote more recently, and files the losers in the dead-letter table. Set it
	// through EnableConflictResolution, which checks the target can support it.
	LastWriteWins bool

	// Conflicts counts changes that lost. Read it after the stream stops.
	Conflicts int

	tables map[string]TableMeta
}

func NewApplier(ctx context.Context, source RowQuerier, target DB) (*Applier, error) {
	a := &Applier{Target: target, Source: source}
	return a, a.reload(ctx)
}

// reload rereads key shapes and column types from the *target*: that is the
// side the generated SQL runs against, so its primary key is the ON CONFLICT
// target and its types are the casts.
func (a *Applier) reload(ctx context.Context) error {
	tables, err := LoadTables(ctx, a.Target)
	if err != nil {
		return err
	}
	a.tables = tables
	return nil
}

// Handle returns a Stream handler bound to ctx. Stream calls it serially, so
// the cached metadata needs no locking.
func (a *Applier) Handle(ctx context.Context) func(WALEvent) error {
	return func(e WALEvent) error { return a.apply(ctx, e) }
}

func (a *Applier) apply(ctx context.Context, e WALEvent) error {
	if e.Table == DLQTable {
		// Never replicate the dead-letter table. With a FOR ALL TABLES
		// publication on the reverse pipeline it would otherwise stream into a
		// database that has no such table, and stall the stream on 42P01.
		return nil
	}

	key := e.Schema + "." + e.Table
	meta, ok := a.tables[key]
	if !ok {
		// A table created on the source after startup. Rereading is enough,
		// provided it also exists on the target.
		if err := a.reload(ctx); err != nil {
			return err
		}
		if meta, ok = a.tables[key]; !ok {
			return fmt.Errorf("%s exists on the source but not on the target; create it "+
				"there before it takes writes (ferryman does not replicate CREATE TABLE)", key)
		}
	}

	var lww time.Time
	if a.LastWriteWins {
		lww = e.CommitTime
	}

	rows, err := apply(ctx, a.Target, meta, e, lww)
	if isUndefinedColumn(err) {
		// The source ran DDL mid-stream. pgoutput does not carry it, but the
		// event that just failed is itself the notification, and it arrives in
		// commit order — so reconciling here is exactly early enough.
		if _, serr := SyncSchema(ctx, a.Source, a.Target); serr != nil {
			return fmt.Errorf("%w; reconciling the schema failed too: %v", err, serr)
		}
		if err := a.reload(ctx); err != nil {
			return err
		}
		rows, err = apply(ctx, a.Target, a.tables[key], e, lww)
	}
	if err != nil {
		return err
	}

	// Only UPDATE and DELETE are counted. A lost INSERT is an upsert whose
	// existing row was newer, which is the outcome last-write-wins is asking
	// for, and an ON CONFLICT DO NOTHING affects no rows on every ordinary
	// replay — neither is a conflict worth filing.
	if a.LastWriteWins && rows == 0 && (e.Op == "UPDATE" || e.Op == "DELETE") {
		a.Conflicts++
		return a.recordConflict(ctx, meta, e)
	}
	return nil
}

func isUndefinedColumn(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42703"
}
