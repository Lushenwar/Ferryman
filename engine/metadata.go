package engine

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Column is one column of a replicated table.
type Column struct {
	Name string
	Type string // format_type output, e.g. "bigint", "jsonb", "timestamp with time zone"
	IsPK bool
}

// TableMeta describes how to address rows of one table. Built by querying the
// catalog rather than assuming every table has a single column called "id",
// which panics on the first composite key or PK-less table it meets
// (danger zone #3).
type TableMeta struct {
	Schema  string
	Table   string
	Columns []Column

	// KeyColumns identifies a row. It is the primary key when there is one.
	//
	// ponytail: a PK-less table falls back to matching on every column, which
	// is what REPLICA IDENTITY FULL gives us. That collapses exact-duplicate
	// rows — two identical rows are indistinguishable, so a DELETE of one
	// removes both. Give such a table a real key if that matters.
	KeyColumns []string
}

// HasPK reports whether KeyColumns came from a primary key. When false, key
// matching is a whole-row comparison and there is no ON CONFLICT target.
func (m TableMeta) HasPK() bool {
	for _, c := range m.Columns {
		if c.IsPK {
			return true
		}
	}
	return false
}

func (m TableMeta) column(name string) (Column, bool) {
	for _, c := range m.Columns {
		if c.Name == name {
			return c, true
		}
	}
	return Column{}, false
}

// Qualified returns the schema-qualified, quoted table name.
func (m TableMeta) Qualified() string {
	return quoteIdent(m.Schema) + "." + quoteIdent(m.Table)
}

const tableMetaQuery = `
SELECT n.nspname,
       c.relname,
       a.attname,
       format_type(a.atttypid, a.atttypmod),
       COALESCE(i.indisprimary, false)
  FROM pg_attribute a
  JOIN pg_class     c ON c.oid = a.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_index i ON i.indrelid = c.oid
                      AND i.indisprimary
                      AND a.attnum = ANY (i.indkey::smallint[])
 WHERE c.relkind = 'r'
   AND n.nspname NOT IN ('pg_catalog', 'information_schema')
   AND a.attnum > 0
   AND NOT a.attisdropped
 ORDER BY n.nspname, c.relname, a.attnum`

// RowQuerier and Execer are the slices of pgx that both *pgxpool.Pool and
// pgx.Tx satisfy, so callers can run the applier standalone or inside their
// own transaction.
type RowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// LoadTables reads column and primary key structure for every user table,
// keyed "schema.table". Call once at startup; relation shapes are assumed
// stable for the life of the migration.
func LoadTables(ctx context.Context, q RowQuerier) (map[string]TableMeta, error) {
	rows, err := q.Query(ctx, tableMetaQuery)
	if err != nil {
		return nil, fmt.Errorf("load table metadata: %w", err)
	}
	defer rows.Close()

	tables := map[string]TableMeta{}
	for rows.Next() {
		var schema, table string
		var col Column
		if err := rows.Scan(&schema, &table, &col.Name, &col.Type, &col.IsPK); err != nil {
			return nil, err
		}
		key := schema + "." + table
		m, ok := tables[key]
		if !ok {
			m = TableMeta{Schema: schema, Table: table}
		}
		m.Columns = append(m.Columns, col)
		tables[key] = m
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for key, m := range tables {
		for _, c := range m.Columns {
			if c.IsPK {
				m.KeyColumns = append(m.KeyColumns, c.Name)
			}
		}
		if len(m.KeyColumns) == 0 {
			for _, c := range m.Columns {
				m.KeyColumns = append(m.KeyColumns, c.Name)
			}
		}
		tables[key] = m
	}
	return tables, nil
}
