// Package engine implements the Ferryman CDC migration pipeline: WAL streaming
// and decoding (this file), idempotent application, snapshot backfill, cutover
// routing, and reverse replication.
package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	DefaultSlot        = "ferryman_slot"
	DefaultPublication = "ferryman_pub"

	// How often we tell postgres which LSN it may discard. Too long and the
	// upstream drops us as a dead standby while WAL piles up on its disk
	// (danger zone #4); too short is just chatter.
	StatusInterval = 10 * time.Second
)

// WALEvent is one decoded row change.
//
// Values are the text-format bytes pgoutput sent, as Go strings, or nil for
// SQL NULL. Postgres parses those same strings back on input, so the applier
// can bind them as query parameters without knowing column types.
type WALEvent struct {
	Op      string // "INSERT", "UPDATE" or "DELETE"
	Schema  string
	Table   string
	Data    map[string]any // new row state; nil for DELETE
	OldData map[string]any // pre-image, present when REPLICA IDENTITY is FULL
	IsToast map[string]bool

	// CommitLSN is the end LSN of the transaction this change belongs to. It is
	// only known once the transaction commits, so it is zero on the event and
	// filled in by the caller if needed; Stream acks by transaction, not by row.
	CommitLSN pglogrepl.LSN

	// CommitTime is when the originating transaction committed, taken from the
	// stream's BEGIN frame. It orders this change against writes made directly
	// on the receiving database, which is what last-write-wins compares.
	CommitTime time.Time
}

// Progress reports how far the applier has durably caught up. Its value is the
// end LSN of the most recent transaction applied without error, so it is a
// statement about the target, not about what has merely been received.
//
// The zero value is ready to use, and a nil *Progress is accepted everywhere,
// which keeps it optional for callers that do not measure lag.
type Progress struct{ v atomic.Uint64 }

func (p *Progress) set(lsn pglogrepl.LSN) {
	if p != nil {
		p.v.Store(uint64(lsn))
	}
}

// LSN is safe to call from another goroutine while a stream is running.
func (p *Progress) LSN() pglogrepl.LSN {
	if p == nil {
		return 0
	}
	return pglogrepl.LSN(p.v.Load())
}

// ReplicationConnect opens a connection in walsender mode. A normal connection
// cannot issue START_REPLICATION, and a walsender connection is a poor fit for
// ordinary queries, so callers keep the two separate.
func ReplicationConnect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, cfg)
}

// EnsurePublication creates the publication if it is absent. FOR ALL TABLES
// keeps the migration in step with schema changes without a table registry.
// ponytail: swap to an explicit table list if you ever need partial migrations.
func EnsurePublication(ctx context.Context, conn *pgconn.PgConn, name string) error {
	res, err := conn.Exec(ctx, "SELECT 1 FROM pg_publication WHERE pubname = "+quoteLiteral(name)).ReadAll()
	if err != nil {
		return fmt.Errorf("look up publication: %w", err)
	}
	if len(res) > 0 && len(res[0].Rows) > 0 {
		return nil
	}
	if err := conn.Exec(ctx, "CREATE PUBLICATION "+quoteIdent(name)+" FOR ALL TABLES").Close(); err != nil {
		return fmt.Errorf("create publication %s: %w", name, err)
	}
	return nil
}

// EnsureSlot creates the logical slot if absent and returns the name of the
// snapshot it exported. That snapshot is what phase 3 backfills from: it is the
// exact database state as of the slot's start LSN, so a backfill reading it and
// a stream starting at that LSN meet with no gap and no overlap.
//
// A pre-existing slot has no retrievable snapshot — postgres discards exported
// snapshots when the creating session ends — so the returned name is empty and
// the caller must not attempt a fresh backfill against it.
func EnsureSlot(ctx context.Context, conn *pgconn.PgConn, slot string) (snapshot string, consistentPoint pglogrepl.LSN, err error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		SnapshotAction: "EXPORT_SNAPSHOT",
		Mode:           pglogrepl.LogicalReplication,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42710" { // duplicate_object
			return "", 0, nil
		}
		return "", 0, fmt.Errorf("create slot %s: %w", slot, err)
	}
	lsn, err := pglogrepl.ParseLSN(res.ConsistentPoint)
	if err != nil {
		return "", 0, fmt.Errorf("parse consistent point %q: %w", res.ConsistentPoint, err)
	}
	return res.SnapshotName, lsn, nil
}

// SlotLSN reports the oldest LSN the slot still needs, i.e. where a restart
// resumes from. Uses a regular connection, not a walsender one.
func SlotLSN(ctx context.Context, conn *pgconn.PgConn, slot string) (pglogrepl.LSN, error) {
	res, err := conn.Exec(ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = "+quoteLiteral(slot)).ReadAll()
	if err != nil {
		return 0, err
	}
	if len(res) == 0 || len(res[0].Rows) == 0 || res[0].Rows[0][0] == nil {
		return 0, fmt.Errorf("slot %s not found", slot)
	}
	return pglogrepl.ParseLSN(string(res[0].Rows[0][0]))
}

// Stream decodes the slot's WAL and invokes handle for every row change, in
// commit order.
//
// handle returning nil means the change is durably applied downstream. The
// stream only reports a position back to postgres at transaction boundaries
// once every change in that transaction succeeded, so a crash mid-transaction
// replays the whole transaction rather than resuming inside it. If handle
// returns an error, Stream stops without acknowledging, and the next run
// redelivers from the last committed position.
//
// Passing startLSN 0 resumes from the slot's own confirmed position. progress
// may be nil; when set, it tracks how far the target has caught up, which is
// what cutover waits on to reach zero lag.
func Stream(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN, progress *Progress, handle func(WALEvent) error) error {
	err := pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			"publication_names '" + publication + "'",
			// Cutover drains lag by dropping a logical decoding message into the
			// stream and waiting to pass it. Without this option pgoutput drops
			// the message, the transaction is empty, and pgoutput suppresses
			// empty transactions — so the marker would never arrive.
			"messages 'true'",
		},
	})
	if err != nil {
		return fmt.Errorf("start replication on %s: %w", slot, err)
	}

	relations := map[uint32]*pglogrepl.RelationMessage{}
	var commitTime time.Time // of the transaction currently being decoded
	acked := startLSN
	nextStatus := time.Now()

	for {
		if !time.Now().Before(nextStatus) {
			if err := sendStatus(ctx, conn, acked); err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			nextStatus = time.Now().Add(StatusInterval)
		}

		// The receive deadline doubles as the keepalive timer: when nothing
		// arrives before the next status is due, the timeout brings us back to
		// the top of the loop to send it.
		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		msg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return fmt.Errorf("receive wal message: %w", err)
		}

		copyData, ok := msg.(*pgproto3.CopyData)
		if !ok {
			if errResp, isErr := msg.(*pgproto3.ErrorResponse); isErr {
				return fmt.Errorf("replication error from server: %s", errResp.Message)
			}
			continue // ParameterStatus, NoticeResponse and friends
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				nextStatus = time.Now() // answer on the next turn of the loop
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse xlogdata: %w", err)
			}
			logical, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse logical message: %w", err)
			}

			switch m := logical.(type) {
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m

			case *pglogrepl.BeginMessage:
				// Announced once, up front, ahead of every row it covers.
				commitTime = m.CommitTime

			case *pglogrepl.CommitMessage:
				// Every change in this transaction was handled without error,
				// so the position is now safe to release.
				acked = m.TransactionEndLSN
				progress.set(acked)
				nextStatus = time.Now()

			case *pglogrepl.InsertMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("insert for unknown relation %d", m.RelationID)
				}
				data, toast := decodeTuple(rel, m.Tuple)
				if err := handle(WALEvent{
					Op: "INSERT", Schema: rel.Namespace, Table: rel.RelationName,
					Data: data, IsToast: toast, CommitTime: commitTime,
				}); err != nil {
					return err
				}

			case *pglogrepl.UpdateMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("update for unknown relation %d", m.RelationID)
				}
				data, toast := decodeTuple(rel, m.NewTuple)
				old, _ := decodeTuple(rel, m.OldTuple)
				if err := handle(WALEvent{
					Op: "UPDATE", Schema: rel.Namespace, Table: rel.RelationName,
					Data: data, OldData: old, IsToast: toast, CommitTime: commitTime,
				}); err != nil {
					return err
				}

			case *pglogrepl.DeleteMessage:
				rel, ok := relations[m.RelationID]
				if !ok {
					return fmt.Errorf("delete for unknown relation %d", m.RelationID)
				}
				old, _ := decodeTuple(rel, m.OldTuple)
				if err := handle(WALEvent{
					Op: "DELETE", Schema: rel.Namespace, Table: rel.RelationName,
					OldData: old, CommitTime: commitTime,
				}); err != nil {
					return err
				}
			}
		}
	}
}

func sendStatus(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lsn,
		WALFlushPosition: lsn,
		WALApplyPosition: lsn,
		ClientTime:       time.Now(),
	})
}

// decodeTuple turns a pgoutput tuple into column name -> value, and separately
// reports which columns arrived as unchanged-TOAST placeholders.
//
// An unchanged TOAST column carries no value at all. It is deliberately absent
// from the returned map rather than present-and-nil, so that a caller which
// ignores IsToast still cannot mistake "not sent" for "set to NULL" and
// overwrite a large value on the target with nothing (danger zone #2).
func decodeTuple(rel *pglogrepl.RelationMessage, t *pglogrepl.TupleData) (map[string]any, map[string]bool) {
	if t == nil {
		return nil, nil
	}
	data := make(map[string]any, len(t.Columns))
	toast := make(map[string]bool)
	for i, col := range t.Columns {
		if i >= len(rel.Columns) {
			break // relation message is stale; extra columns are unnameable
		}
		name := rel.Columns[i].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			data[name] = nil
		case pglogrepl.TupleDataTypeToast:
			toast[name] = true
		default:
			data[name] = string(col.Data)
		}
	}
	return data, toast
}

func quoteIdent(s string) string   { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func quoteLiteral(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }
