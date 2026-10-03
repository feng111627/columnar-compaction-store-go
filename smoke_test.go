package columnar

import (
	"errors"
	"testing"
)

func TestSmokeWriteReadProjectFilter(t *testing.T) {
	dir := t.TempDir()
	tbl, err := Open(dir, "orders")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tbl.Close()

	if _, err := tbl.AddColumn("id", TypeInt64); err != nil {
		t.Fatalf("AddColumn id: %v", err)
	}
	if _, err := tbl.AddColumn("city", TypeString); err != nil {
		t.Fatalf("AddColumn city: %v", err)
	}

	b := &Batch{Columns: []*Column{
		{Name: "id", Type: TypeInt64, I64s: []int64{1, 2, 3, 4}, Nulls: []bool{false, false, false, false}},
		{Name: "city", Type: TypeString, Strs: []string{"BJ", "SH", "BJ", "SZ"}, Nulls: []bool{false, false, false, false}},
	}}
	if _, err := tbl.Append(b); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// 只投影 id 列，并下推 city = "BJ" 过滤。
	r, err := tbl.Reader([]string{"id"}, &Filter{Predicates: []Predicate{
		{Column: "city", Op: OpEq, Literal: Value{Type: TypeString, Str: "BJ", Valid: true}},
	}})
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer r.Close()

	var got []int64
	for {
		rs, ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		v := rs.Row()["id"]
		if _, present := rs.Row()["city"]; present {
			t.Fatalf("过滤列 city 不应出现在投影结果中")
		}
		got = append(got, v.I64)
	}
	want := []int64{1, 3}
	if len(got) != len(want) {
		t.Fatalf("过滤结果 = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("过滤结果 = %v, want %v", got, want)
		}
	}

	// 错误原因可区分：缺失列 vs 类型不符。
	if _, err := tbl.Reader([]string{"nope"}, nil); !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("缺失投影列应报 ErrColumnNotFound, got %v", err)
	}
	if _, err := tbl.Reader(nil, &Filter{Predicates: []Predicate{
		{Column: "id", Op: OpEq, Literal: Value{Type: TypeString, Str: "1", Valid: true}},
	}}); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("过滤字面量类型不符应报 ErrTypeMismatch, got %v", err)
	}
}
