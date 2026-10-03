package columnar

import (
	"errors"
	"testing"
)

func TestEdgeCases(t *testing.T) {
	tw := &testingWriter{t}

	// 空表读取。
	t1, err := Open(t.TempDir(), "empty")
	if err != nil {
		t.Fatal(err)
	}
	r, err := t1.Reader(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rows := collectRows(t, r); len(rows) != 0 {
		t.Fatalf("空表应有 0 行, got %d", len(rows))
	}
	r.Close()
	if _, err := t1.Compact(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("空表合并应报 ErrInvalidArgument, got %v", err)
	}
	t1.Close()
	tw.logf("[边界 空表] 结论: 空表读取 0 行；空表 Compact -> ErrInvalidArgument")

	// 单文件无需合并。
	t2, _ := Open(t.TempDir(), "one")
	t2.AddColumn("x", TypeInt64)
	t2.Append(&Batch{Columns: []*Column{colI64("x", []int64{1}, nil)}})
	if _, err := t2.Compact(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("单文件合并应报 ErrInvalidArgument, got %v", err)
	}
	tw.logf("[边界 单文件] 结论: 仅 1 个文件时 Compact -> ErrInvalidArgument")
	t2.Close()

	// NULL 值写读保持，且 NULL 不参与比较谓词。
	t3, _ := Open(t.TempDir(), "nulls")
	t3.AddColumn("a", TypeInt64)
	t3.Append(&Batch{Columns: []*Column{
		colI64("a", []int64{0, 5, 0}, []bool{false, false, true}),
	}})
	r3, err := t3.Reader([]string{"a"}, &Filter{Predicates: []Predicate{
		{Column: "a", Op: OpLt, Literal: vI64(10)},
	}})
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, r3)
	r3.Close()
	if len(rows) != 2 {
		t.Fatalf("a<10 应命中 2 个非空行, got %d", len(rows))
	}
	r4, err := t3.Reader([]string{"a"}, &Filter{Predicates: []Predicate{
		{Column: "a", Op: OpIsNull},
	}})
	if err != nil {
		t.Fatal(err)
	}
	nullRows := collectRows(t, r4)
	r4.Close()
	if len(nullRows) != 1 || nullRows[0]["a"].Valid {
		t.Fatalf("IS NULL 应命中 1 行, got %d", len(nullRows))
	}
	tw.logf("[边界 NULL] 输入: a=[1,5,NULL]；结论: a<10 命中 2 行，IS NULL 命中 1 行")

	// 重开后仍可继续追加并合并。
	dir := t.TempDir()
	t4, _ := Open(dir, "reopen")
	t4.AddColumn("k", TypeInt64)
	t4.Append(&Batch{Columns: []*Column{colI64("k", []int64{1}, nil)}})
	t4.Close()
	t5, err := Open(dir, "reopen")
	if err != nil {
		t.Fatal(err)
	}
	t5.Append(&Batch{Columns: []*Column{colI64("k", []int64{2}, nil)}})
	if _, err := t5.Compact(); err != nil {
		t.Fatalf("重开后合并失败: %v", err)
	}
	r5, _ := t5.Reader(nil, nil)
	if rows := collectRows(t, r5); len(rows) != 2 {
		t.Fatalf("重开追加合并后应有 2 行, got %d", len(rows))
	}
	r5.Close()
	t5.Close()
	tw.logf("[边界 重开] 结论: Close/Open 后可继续追加、ID 不冲突、合并正常")

	// 空批次、空表名等参数错误。
	if _, err := Open("", "x"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("空 dir 应报 ErrInvalidArgument, got %v", err)
	}
	if _, err := t3.Append(&Batch{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("空批次应报 ErrInvalidArgument, got %v", err)
	}
	tw.logf("[边界 参数] 结论: 空 dir、空批次 -> ErrInvalidArgument")
}
