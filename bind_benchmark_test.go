package pg

import (
	"maps"
	"testing"

	// Packages
	pgx "github.com/jackc/pgx/v5"
)

func Benchmark_Bind_Copy_NoArgs(b *testing.B) {
	bind := NewBind(
		"a", "b",
		"c", 123,
		"d", true,
		"e", []string{"x", "y", "z"},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bind.Copy()
	}
}

func Benchmark_Bind_Copy_NoArgs_PreviousStyle(b *testing.B) {
	bind := NewBind(
		"a", "b",
		"c", 123,
		"d", true,
		"e", []string{"x", "y", "z"},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = previousStyleCopy(bind)
	}
}

func Benchmark_Bind_Copy_WithPairs(b *testing.B) {
	bind := NewBind(
		"a", "b",
		"c", 123,
		"d", true,
		"e", []string{"x", "y", "z"},
	)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bind.Copy("f", "g", "h", 456)
	}
}

func previousStyleCopy(bind *Bind) *Bind {
	varsCopy := func() pgx.NamedArgs {
		bind.RLock()
		defer bind.RUnlock()
		c := make(pgx.NamedArgs, len(bind.vars))
		maps.Copy(c, bind.vars)
		return c
	}()
	return &Bind{vars: varsCopy, dblink: bind.dblink}
}
