package columnar

import (
	"context"
	"errors"
	"testing"
)

// collect 把扫描结果里某列的值按行序收集出来（含 NULL）。
func colVals(rows []Row, name string) []any {
	out := make([]any, len(rows))
	for i, r := range rows {
		out[i] = r[name]
	}
	return out
}

func anySliceEq(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestRenameColumnReadContinuity 列改名：历史数据在新名字下连续可读，
// 带列裁剪与不带列裁剪结果一致，旧名字立即失效。
func TestRenameColumnReadContinuity(t *testing.T) {
	biz := "列生命周期-改名连续可读"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "status", Type: TypeInt64, Nullable: true},
	}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})

	// 改名前的历史数据（老名字 status 落盘）
	for i, v := range []int64{10, 20, 30} {
		if err := s.Append(ctx, Row{"id": int64(i + 1), "status": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	if err := s.RenameColumn(ctx, "status", "state"); err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "改名 status->state", "3行历史数据在老名字下", "改名成功")

	// 改名后的新数据（新名字 state 落盘）
	for i, v := range []int64{40, 50} {
		if err := s.Append(ctx, Row{"id": int64(i + 4), "state": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	want := []any{int64(10), int64(20), int64(30), int64(40), int64(50)}

	// 全列读：改名前后数据连续
	full := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "全列读 state 列", "5行(3老+2新)", colVals(full, "state"))
	if !anySliceEq(colVals(full, "state"), want) {
		t.Fatalf("rename continuity broken: %v", colVals(full, "state"))
	}

	// 列裁剪读：结果必须与全列读一致
	pruned := scanAll(t, s, ScanOptions{Columns: []string{"state"}})
	logCase(t, biz, "列裁剪读 state 列", "Columns=[state]", colVals(pruned, "state"))
	if !anySliceEq(colVals(pruned, "state"), want) {
		t.Fatalf("pruned read differs from full read: %v", colVals(pruned, "state"))
	}

	// 过滤下推到新老两段：跨段连续
	filtered := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "state", Op: OpGreaterEqual, Value: int64(30)}}})
	logCase(t, biz, "过滤 state>=30", "跨新老两段", colVals(filtered, "state"))
	if !anySliceEq(colVals(filtered, "state"), []any{int64(30), int64(40), int64(50)}) {
		t.Fatalf("filtered read wrong: %v", colVals(filtered, "state"))
	}

	// 旧名字立即失效
	_, err := s.Scan(ctx, ScanOptions{Columns: []string{"status"}})
	logCase(t, biz, "按旧名字投影", "Columns=[status]", err)
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("old name must be gone, got %v", err)
	}

	// 重开后仍然连续（清单里的结构历史持久化）
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	reopened := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重开后全列读", "reopen", colVals(reopened, "state"))
	if !anySliceEq(colVals(reopened, "state"), want) {
		t.Fatalf("reopen read wrong: %v", colVals(reopened, "state"))
	}

	// 合并把新老段重写为一段后，数据依旧连续
	if err := s2.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	merged := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "合并后全列读", "post-compaction", colVals(merged, "state"))
	if !anySliceEq(colVals(merged, "state"), want) {
		t.Fatalf("post-compaction read wrong: %v", colVals(merged, "state"))
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestColumnRebirthReadsNullOnOldData 同名重生：删除 int64 列 code 后以同名
// string 列重新上线。旧数据上该列必须读成 NULL；老列的 int64 字节在任何
// 读取方式下都不能被当成新列的值；类型不兼容也只影响该列，整表可读。
func TestColumnRebirthReadsNullOnOldData(t *testing.T) {
	biz := "列生命周期-同名重生隔离"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "code", Type: TypeInt64, Nullable: true}, // 老 code：数值状态码
	}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})

	for i, v := range []int64{100, 200, 300} {
		if err := s.Append(ctx, Row{"id": int64(i + 1), "code": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 下线再以同名重生：新 code 是文本标签，类型与老列完全不兼容
	if err := s.DropColumn(ctx, "code"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddColumn(ctx, Column{Name: "code", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "code int64 下线 -> 同名 string 重生", "3行历史数据含老code字节", "重生成功")

	for i, v := range []string{"ok", "bad"} {
		if err := s.Append(ctx, Row{"id": int64(i + 4), "code": v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	want := []any{nil, nil, nil, "ok", "bad"}

	// 全列读：老行 code=NULL，新行为文本；类型不兼容没有拖垮整表
	full := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "全列读 code 列", "3老行+2新行", colVals(full, "code"))
	if !anySliceEq(colVals(full, "code"), want) {
		t.Fatalf("rebirth isolation broken: %v", colVals(full, "code"))
	}
	if !int64SliceEq(idsOf(full), []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("id column must stay intact: %v", idsOf(full))
	}

	// 列裁剪读：与全列读一致，老字节不会被错位读出
	pruned := scanAll(t, s, ScanOptions{Columns: []string{"code"}})
	logCase(t, biz, "列裁剪读 code 列", "Columns=[code]", colVals(pruned, "code"))
	if !anySliceEq(colVals(pruned, "code"), want) {
		t.Fatalf("pruned read differs: %v", colVals(pruned, "code"))
	}

	// 过滤新列：老行的 NULL 不匹配任何比较，老 int64 值绝不参与比较
	filtered := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "code", Op: OpEqual, Value: "ok"}}})
	logCase(t, biz, "过滤 code=='ok'", "老行code=NULL", idsOf(filtered))
	if !int64SliceEq(idsOf(filtered), []int64{4}) {
		t.Fatalf("filter leaked old values or lost new rows: %v", idsOf(filtered))
	}

	// 重开 + 合并后语义不变
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 2})
	reopened := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重开后全列读", "reopen", colVals(reopened, "code"))
	if !anySliceEq(colVals(reopened, "code"), want) {
		t.Fatalf("reopen read wrong: %v", colVals(reopened, "code"))
	}
	if err := s2.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	merged := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "合并后全列读", "post-compaction", colVals(merged, "code"))
	if !anySliceEq(colVals(merged, "code"), want) {
		t.Fatalf("post-compaction read wrong: %v", colVals(merged, "code"))
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestRenameColumnRules 改名的参数校验规则。
func TestRenameColumnRules(t *testing.T) {
	biz := "列生命周期-改名规则"
	ctx := context.Background()
	s := mustCreate(t, t.TempDir(), baseSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s.Close()

	err := s.RenameColumn(ctx, "nope", "x")
	logCase(t, biz, "改名列不存在", "rename nope->x", err)
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("want ErrColumnNotFound, got %v", err)
	}

	err = s.RenameColumn(ctx, "score", "name")
	logCase(t, biz, "新名字已被占用", "rename score->name", err)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}

	err = s.RenameColumn(ctx, "score", "")
	logCase(t, biz, "新名字为空", "rename score->''", err)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}

	verBefore := s.Schema().Version
	if err := s.RenameColumn(ctx, "score", "score"); err != nil {
		t.Fatalf("no-op rename must succeed: %v", err)
	}
	logCase(t, biz, "同名改名幂等", "rename score->score", s.Schema().Version)
	if s.Schema().Version != verBefore {
		t.Fatalf("no-op rename must not bump version: %d -> %d", verBefore, s.Schema().Version)
	}
}
