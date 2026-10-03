package columnar

import (
	"errors"
	"io"
	"testing"
)

func mustCreate(t *testing.T, dir string, sch Schema, opt Options) *Store {
	t.Helper()
	s, err := Create(dir, sch, opt)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return s
}

func mustOpen(t *testing.T, dir string, opt Options) *Store {
	t.Helper()
	s, err := Open(dir, opt)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return s
}

func scanAll(t *testing.T, s *Store, opts ScanOptions) []Row {
	t.Helper()
	sc, err := s.Scan(t.Context(), opts)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer sc.Close()
	var rows []Row
	for {
		r, err := sc.Next()
		if errors.Is(err, io.EOF) {
			return rows
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		rows = append(rows, r)
	}
}

func idsOf(rows []Row) []int64 {
	out := make([]int64, len(rows))
	for i, r := range rows {
		out[i] = r["id"].(int64)
	}
	return out
}
