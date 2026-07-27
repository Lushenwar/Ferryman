package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

// Router is the traffic switch: a TCP proxy in front of the database whose
// backend can be repointed at runtime.
//
// The spec offers PgBouncer or a dynamic proxy. PgBouncer is the wrong tool for
// the moving part here — its admin console has PAUSE and RESUME, but no command
// that repoints a pool at a different host; that needs a rewritten pgbouncer.ini
// and a RELOAD, which is a config-management problem rather than a cutover
// primitive. The proxy is small enough to own and makes the guarantee testable.
//
// The guarantee is that no client connection is ever refused. While paused, an
// incoming connection is accepted and held, not rejected, so a client sees
// latency instead of an error. Pause also waits for connections already talking
// to the old backend to finish, so nothing is cut mid-query.
//
// ponytail: this proxies bytes and does not parse the postgres wire protocol,
// so "in flight" means an open connection rather than an open transaction. With
// short-lived pooled connections that is the same thing. A client that holds one
// connection open indefinitely will stall the drain until DrainTimeout, at which
// point cutover aborts and returns traffic to the source rather than cutting the
// connection. Parsing the protocol to track transaction boundaries is the
// upgrade path if long-lived sessions become normal.
type Router struct {
	mu       sync.Mutex
	cond     *sync.Cond
	backend  string
	paused   bool
	closed   bool
	inflight sync.WaitGroup

	ln net.Listener
}

// NewRouter starts listening immediately so callers can read Addr before
// serving. listenAddr may use port 0 to get an arbitrary free port.
func NewRouter(listenAddr, backend string) (*Router, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	r := &Router{backend: backend, ln: ln}
	r.cond = sync.NewCond(&r.mu)
	return r, nil
}

func (r *Router) Addr() string { return r.ln.Addr().String() }

// Serve accepts until Close. It is meant to run in its own goroutine.
func (r *Router) Serve() {
	for {
		client, err := r.ln.Accept()
		if err != nil {
			return // listener closed
		}
		go r.proxy(client)
	}
}

// Backend reports the address traffic is currently sent to.
func (r *Router) Backend() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.backend
}

// SetBackend repoints subsequent connections. Call it while paused; changing
// the backend under live traffic would send some connections to each database.
func (r *Router) SetBackend(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.backend = addr
}

// Pause stops handing out backend connections and waits for the ones already
// open to close, so the databases are quiescent before the switch.
//
// New clients are held, not refused. If the wait exceeds the context deadline
// the router stays paused and the error is returned; the caller decides whether
// to resume against the old backend or the new one.
func (r *Router) Pause(ctx context.Context) error {
	r.mu.Lock()
	r.paused = true
	r.mu.Unlock()

	// Safe to Wait concurrently with the accept loop: acquire increments the
	// counter under the same mutex that gates paused, so no new Add can start
	// once paused is set.
	drained := make(chan struct{})
	go func() {
		r.inflight.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("draining open connections: %w", ctx.Err())
	}
}

// Resume releases every held connection against the current backend.
func (r *Router) Resume() {
	r.mu.Lock()
	r.paused = false
	r.mu.Unlock()
	r.cond.Broadcast()
}

func (r *Router) Close() error {
	r.mu.Lock()
	r.closed = true
	r.paused = false
	r.mu.Unlock()
	r.cond.Broadcast()
	return r.ln.Close()
}

// acquire blocks while paused, then registers one in-flight connection.
func (r *Router) acquire() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.paused && !r.closed {
		r.cond.Wait()
	}
	if r.closed {
		return "", false
	}
	r.inflight.Add(1)
	return r.backend, true
}

func (r *Router) proxy(client net.Conn) {
	defer client.Close()

	backend, ok := r.acquire()
	if !ok {
		return
	}
	defer r.inflight.Done()

	server, err := net.Dial("tcp", backend)
	if err != nil {
		return // client sees a closed connection; nothing better to send in raw TCP
	}
	defer server.Close()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(server, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, server); done <- struct{}{} }()
	<-done // either direction ending means the session is over
}

// CutoverConfig describes one forward cutover.
type CutoverConfig struct {
	Router   *Router
	Progress *Progress // updated by the running Stream

	SourceDSN string // direct, not through the router
	TargetDSN string

	TargetBackend string        // host:port the router should point at afterwards
	DrainTimeout  time.Duration // budget for open connections plus replication lag
}

// Cutover moves live traffic from source to target with no refused connections.
//
//	pause traffic -> let open connections finish -> drain replication lag to
//	zero -> sync sequences -> repoint the router -> resume
//
// Every failure path resumes traffic before returning. Leaving the router
// paused would turn a failed migration into an outage, which is the one
// outcome the whole design exists to avoid. On failure traffic resumes against
// the source, which is still authoritative because nothing was switched.
func Cutover(ctx context.Context, cfg CutoverConfig) (err error) {
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.DrainTimeout)
	defer cancel()

	original := cfg.Router.Backend()
	defer func() {
		if err != nil {
			cfg.Router.SetBackend(original)
		}
		cfg.Router.Resume()
	}()

	if err := cfg.Router.Pause(ctx); err != nil {
		return err
	}

	src, err := pgconn.Connect(ctx, cfg.SourceDSN)
	if err != nil {
		return fmt.Errorf("connect source for cutover: %w", err)
	}
	defer src.Close(context.Background())

	// Danger zone #5: a long-running query holding a lock must not hang the
	// cutover indefinitely. Administrative statements fail fast instead.
	if err := src.Exec(ctx, "SET statement_timeout = '2s'").Close(); err != nil {
		return fmt.Errorf("set statement timeout: %w", err)
	}

	// Traffic is stopped and open connections have closed, so the source will
	// produce no further row changes. Waiting for the applier to reach
	// pg_current_wal_lsn() would not work: that position includes WAL the
	// stream never delivers — checkpoints, vacuum, activity in other databases —
	// so the applier would sit just short of it forever.
	//
	// Instead drop a marker into the stream itself and wait for the applier to
	// pass it. Everything the source committed before this point is ordered
	// ahead of the marker, so overtaking it means zero lag by construction.
	marker, err := emitCutoverMarker(ctx, src)
	if err != nil {
		return err
	}
	// Strictly past: the marker's own transaction commits after the record.
	if err := waitForLSN(ctx, cfg.Progress, marker+1); err != nil {
		return err
	}

	// After the drain so it picks up values CDC advanced past, not just the
	// ones the backfill copied.
	if err := SyncSequences(ctx, cfg.SourceDSN, cfg.TargetDSN); err != nil {
		return fmt.Errorf("sync sequences: %w", err)
	}

	cfg.Router.SetBackend(cfg.TargetBackend)
	return nil
}

// emitCutoverMarker writes a logical decoding message and returns its LSN.
//
// The message carries no data and touches no table; its only job is to give the
// transaction content, since pgoutput suppresses empty transactions and an empty
// one would never advance the applier. Stream requests messages from pgoutput
// for exactly this reason.
func emitCutoverMarker(ctx context.Context, conn *pgconn.PgConn) (pglogrepl.LSN, error) {
	res, err := conn.Exec(ctx,
		"SELECT pg_logical_emit_message(true, 'ferryman', 'cutover')::text").ReadAll()
	if err != nil {
		return 0, fmt.Errorf("emit cutover marker: %w", err)
	}
	if len(res) == 0 || len(res[0].Rows) == 0 {
		return 0, errors.New("source returned no marker position")
	}
	return pglogrepl.ParseLSN(string(res[0].Rows[0][0]))
}

// waitForLSN blocks until the applier has caught up to want.
func waitForLSN(ctx context.Context, p *Progress, want pglogrepl.LSN) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if p.LSN() >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("replication lag did not reach zero: applied %s, need %s: %w",
				p.LSN(), want, ctx.Err())
		case <-ticker.C:
		}
	}
}
