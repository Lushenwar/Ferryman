package main

import (
	"testing"

	"github.com/Lushenwar/Ferryman/engine"
)

// The orchestration in migrate needs live databases and is covered by the
// engine's integration tests. What is only here is the plumbing that turns
// config into arguments, and every one of these is a silent wrong answer rather
// than a crash if it breaks: a bad hostPort routes traffic at the wrong database,
// and an unordered tableList makes backfill chunk layout differ run to run.

func TestHostPortMatchesTheDSN(t *testing.T) {
	for _, tc := range []struct{ dsn, want string }{
		{"postgres://u:p@10.0.0.5:5433/db", "10.0.0.5:5433"},
		{"postgres://u:p@example.com/db", "example.com:5432"}, // default port
		{"host=127.0.0.1 port=5434 user=u", "127.0.0.1:5434"}, // keyword form
	} {
		got, err := hostPort(tc.dsn)
		if err != nil {
			t.Fatalf("hostPort(%q): %v", tc.dsn, err)
		}
		if got != tc.want {
			t.Errorf("hostPort(%q) = %q, want %q", tc.dsn, got, tc.want)
		}
	}
	if _, err := hostPort("::not a dsn::"); err == nil {
		t.Error("hostPort accepted a malformed DSN")
	}
}

func TestTableListIsOrderedAndExcludesTheDLQ(t *testing.T) {
	in := map[string]engine.TableMeta{
		"public.orders":             {Schema: "public", Table: "orders"},
		"public.accounts":           {Schema: "public", Table: "accounts"},
		"public." + engine.DLQTable: {Schema: "public", Table: engine.DLQTable},
		"billing.invoices":          {Schema: "billing", Table: "invoices"},
	}
	got := tableList(in)
	want := []string{"billing.invoices", "public.accounts", "public.orders"}
	if len(got) != len(want) {
		t.Fatalf("got %d tables, want %d: %v", len(got), len(want), got)
	}
	for i, w := range want {
		if k := got[i].Schema + "." + got[i].Table; k != w {
			t.Errorf("table %d = %q, want %q", i, k, w)
		}
	}
}

func TestByteCountReadsAsSizes(t *testing.T) {
	for _, tc := range []struct {
		n    int64
		want string
	}{
		{0, "0 B"},
		{999, "999 B"},
		{1024, "1.0 KiB"},
		{8 << 20, "8.0 MiB"}, // the -max-lag default
		{3 << 30, "3.0 GiB"},
	} {
		if got := byteCount(tc.n); got != tc.want {
			t.Errorf("byteCount(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}
