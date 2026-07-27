package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// DLQTable holds changes the applier could not land. It is excluded from
// replication by name so its rows cannot travel back over the reverse pipeline
// into a database that has no such table.
const DLQTable = "ferryman_dlq"

// SetReadOnly turns ordinary writes to a database on or off.
//
// This is the cheap half of conflict handling, and the half that actually
// matters. During the rollback window the source is still reachable, and a
// background job or a client that never learned about the cutover will happily
// write to it — so the fix is to stop the database taking those writes, not to
// reconcile them afterwards.
//
// Two limits worth knowing before relying on it:
//
//   - default_transaction_read_only applies to sessions opened after this
//     returns. Existing ones keep writing until they reconnect, which after a
//     cutover they have already had to do.
//   - The setting is USERSET, so any session can turn it back off. That makes
//     this a guardrail against accidents, not a security control — and the
//     accidental writer is precisely the case in hand. The reverse applier is
//     the one session that *should* override it, with
//     SET default_transaction_read_only = off; see EnableConflictResolution.
func SetReadOnly(ctx context.Context, dsn string, readOnly bool) error {
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect to set read-only: %w", err)
	}
	defer conn.Close(context.Background())

	// ALTER DATABASE is itself refused inside a read-only transaction, so
	// without this the flag could be set but never cleared. The session-level
	// setting overrides the database-level one, and being USERSET is exactly
	// what makes that possible.
	if err := conn.Exec(ctx, "SET default_transaction_read_only = off").Close(); err != nil {
		return fmt.Errorf("make this session writable: %w", err)
	}

	res, err := conn.Exec(ctx, "SELECT current_database()").ReadAll()
	if err != nil || len(res) == 0 || len(res[0].Rows) == 0 {
		return fmt.Errorf("identify database: %w", err)
	}
	setting := "off"
	if readOnly {
		setting = "on"
	}
	stmt := fmt.Sprintf("ALTER DATABASE %s SET default_transaction_read_only = %s",
		quoteIdent(string(res[0].Rows[0][0])), setting)
	if err := conn.Exec(ctx, stmt).Close(); err != nil {
		return fmt.Errorf("%s: %w", stmt, err)
	}
	return nil
}

// EnableConflictResolution prepares an applier to resolve conflicts by
// last-write-wins, and reports whether the database can actually do so.
//
// It creates the dead-letter table and opts this session out of any read-only
// default set by SetReadOnly — the replicated stream is the one writer that
// must still get through.
//
// track_commit_timestamp cannot be turned on from here; it needs a restart. If
// it is off, this returns an error rather than enabling a comparison that would
// silently answer "unknown" for every row.
func (a *Applier) EnableConflictResolution(ctx context.Context) error {
	rows, err := a.Target.Query(ctx, "SELECT current_setting('track_commit_timestamp')")
	if err != nil {
		return fmt.Errorf("read track_commit_timestamp: %w", err)
	}
	var setting string
	if rows.Next() {
		err = rows.Scan(&setting)
	}
	rows.Close()
	if err != nil {
		return err
	}
	if setting != "on" {
		return fmt.Errorf("last-write-wins needs track_commit_timestamp = on for the target; " +
			"it is off, and changing it requires a restart")
	}

	// default_transaction_read_only, not transaction_read_only: the latter binds
	// to the current transaction, and under autocommit that is the one statement
	// setting it.
	if _, err := a.Target.Exec(ctx, "SET default_transaction_read_only = off"); err != nil {
		return fmt.Errorf("opt the applier out of read-only: %w", err)
	}
	if _, err := a.Target.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS `+quoteIdent(DLQTable)+` (
			id           bigserial PRIMARY KEY,
			recorded_at  timestamptz NOT NULL DEFAULT now(),
			op           text        NOT NULL,
			table_name   text        NOT NULL,
			row_key      jsonb       NOT NULL,
			commit_time  timestamptz,
			reason       text        NOT NULL
		)`); err != nil {
		return fmt.Errorf("create %s: %w", DLQTable, err)
	}
	a.LastWriteWins = true
	return nil
}

// recordConflict files a change that did not land.
//
// ponytail: entries are advisory, not a work queue. A replayed DELETE after an
// interrupted transaction also reaches here, having correctly removed nothing,
// so a row in the table means "look at this", not "this was lost".
// `SELECT * FROM ferryman_dlq` is the review interface; if that stops being
// enough, the next step is a resolved_at column, not a service.
func (a *Applier) recordConflict(ctx context.Context, meta TableMeta, e WALEvent) error {
	row := identity(e)
	key := make(map[string]any, len(meta.KeyColumns))
	for _, k := range meta.KeyColumns {
		key[k] = row[k]
	}
	encoded, err := json.Marshal(key)
	if err != nil {
		return err
	}

	var commit any
	if !e.CommitTime.IsZero() {
		commit = e.CommitTime
	}
	_, err = a.Target.Exec(ctx, `
		INSERT INTO `+quoteIdent(DLQTable)+` (op, table_name, row_key, commit_time, reason)
		VALUES ($1, $2, $3, $4, $5)`,
		e.Op, meta.Qualified(), string(encoded), commit,
		"target row is newer than the replicated change, or no longer present")
	if err != nil {
		return fmt.Errorf("record conflict for %s: %w", meta.Qualified(), err)
	}
	return nil
}
