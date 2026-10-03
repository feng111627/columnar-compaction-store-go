package columnar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestSmokeEndToEnd(t *testing.T) {
	dir := t.TempDir()
	s, err := Create(dir, Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "name", Type: TypeString, Nullable: true},
	}}, Options{FlushThreshold: 2, CompactionMinSegments: 2})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for i := int64(1); i <= 5; i++ {
		if err := s.Append(ctx, Row{"id": i, "name": fmt.Sprintf("n%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	sc, err := s.Scan(ctx, ScanOptions{Filters: []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(3)}}})
	if err != nil {
		t.Fatal(err)
	}
	var got []int64
	for {
		r, err := sc.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, r["id"].(int64))
	}
	sc.Close()
	t.Logf("filter id>=3 got ids=%v", got)
	if len(got) != 3 || got[0] != 3 {
		t.Fatalf("unexpected %v", got)
	}
	t.Logf("segments before compact=%d", s.SegmentCount())
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("segments after compact=%d", s.SegmentCount())
	if s.SegmentCount() != 1 {
		t.Fatalf("want 1 segment, got %d", s.SegmentCount())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
