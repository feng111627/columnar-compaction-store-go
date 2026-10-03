package columnar

import (
	"context"
	"errors"
	"testing"
)

// identitySchema: id int64 非空；status 可空（先 int64，后续演进出改名与重生）。
func identitySchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "status", Type: TypeInt64, Nullable: true},
		{Name: "note", Type: TypeString, Nullable: true},
	}}
}

// TestColumnRenameHistoryContinuous 改名后历史数据在各种读取方式下连续可读。
func TestColumnRenameHistoryContinuous(t *testing.T) {
	biz := "列生命周期-改名历史连续"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, identitySchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})

	// 第一段：旧名字 status 写入历史数据
	for i, st := range []int64{10, 20, 30} {
		if err := s.Append(ctx, Row{"id": int64(i + 1), "status": st, "note": "old"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	oldID := s.m.SchemaHistory[0].Columns[1].ID
	logCase(t, biz, "初始status列身份", "status", oldID)

	// 改名 status -> state，沿用同一身份
	if err := s.RenameColumn(ctx, "status", "state"); err != nil {
		t.Fatal(err)
	}
	cur := s.Schema()
	newCol, ok := cur.Column("state")
	logCase(t, biz, "RenameColumn(status->state)", "state ID", newCol.ID)
	if !ok || newCol.ID != oldID {
		t.Fatalf("renamed column must keep identity id=%d, got %d", oldID, newCol.ID)
	}
	if _, still := cur.Column("status"); still {
		t.Fatal("old name must no longer exist in current schema")
	}

	// 改名后用新名字写第二段
	for i, st := range []int64{40, 50} {
		if err := s.Append(ctx, Row{"id": int64(i + 4), "state": st, "note": "new"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 旧名字写入必须被拒绝
	err := s.Append(ctx, Row{"id": int64(99), "status": int64(1)})
	logCase(t, biz, "改名后用旧名字写入被拒", "status", err)
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("want ErrColumnNotFound writing old name, got %v", err)
	}

	// 三种读取方式：全列、只投影新名字、带过滤
	full := scanAll(t, s, ScanOptions{})
	projected := scanAll(t, s, ScanOptions{Columns: []string{"state"}})
	filtered := scanAll(t, s, ScanOptions{
		Columns: []string{"id", "state"},
		Filters: []Filter{{Column: "state", Op: OpGreaterEqual, Value: int64(30)}},
	})

	wantStates := []any{int64(10), int64(20), int64(30), int64(40), int64(50)}
	gotStates := []any{}
	for _, r := range full {
		gotStates = append(gotStates, r["state"])
		if _, present := r["status"]; present {
			t.Fatalf("old name status must not appear in result rows: %v", r)
		}
	}
	logCase(t, biz, "全列读：旧段+新段经同一身份读出", "10..50", gotStates)
	if len(full) != 5 || !anySliceEq(gotStates, wantStates) {
		t.Fatalf("rename read broken: %v", gotStates)
	}
	pj := []any{}
	for _, r := range projected {
		if len(r) != 1 {
			t.Fatalf("projected row must have 1 key, got %v", r)
		}
		pj = append(pj, r["state"])
	}
	logCase(t, biz, "列裁剪只读state", "10..50", pj)
	if !anySliceEq(pj, wantStates) {
		t.Fatalf("projected rename read broken: %v", pj)
	}
	fids := idsOf(filtered)
	logCase(t, biz, "带过滤state>=30（过滤列跨新旧段）", "ids 3,4,5", fids)
	if !int64SliceEq(fids, []int64{3, 4, 5}) {
		t.Fatalf("filter across rename broken: %v", fids)
	}

	// 合并后语义仍连续
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after := scanAll(t, s, ScanOptions{})
	got := []any{}
	for _, r := range after {
		got = append(got, r["state"])
	}
	logCase(t, biz, "合并后改名列仍连续", "10..50", got)
	if !anySliceEq(got, wantStates) {
		t.Fatalf("post-compaction rename values broken: %v", got)
	}

	// 重开后仍可连续读
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	reopened := scanAll(t, s2, ScanOptions{})
	got = got[:0]
	for _, r := range reopened {
		got = append(got, r["state"])
	}
	logCase(t, biz, "重开后改名列仍连续", "10..50", got)
	if !anySliceEq(got, wantStates) {
		t.Fatalf("reopen rename values broken: %v", got)
	}
	_ = s2.Close()
}

// TestColumnRebornSameNameOldBytesInvisible 同名重生：旧数据在新列上必须读
// 成 NULL，且新类型不兼容也不影响整表；带/不带列裁剪结果一致。
func TestColumnRebornSameNameOldBytesInvisible(t *testing.T) {
	biz := "列生命周期-同名重生旧字节不可见"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, identitySchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})

	// 旧身份：status 是一组数值状态码
	if err := s.Append(ctx, Row{"id": int64(1), "status": int64(909), "note": "old-code"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Row{"id": int64(2), "status": nil, "note": "old-null"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	oldID := s.m.SchemaHistory[0].Columns[1].ID

	// 下线后用同名重新上线，含义与类型完全改变（int64 -> string）
	if err := s.DropColumn(ctx, "status"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddColumn(ctx, Column{Name: "status", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	cur := s.Schema()
	newCol, _ := cur.Column("status")
	logCase(t, biz, "同名重生分配新身份", map[string]any{"oldID": oldID, "newID": newCol.ID, "newType": newCol.Type}, newCol.Type)
	if newCol.ID == oldID || newCol.ID == 0 {
		t.Fatalf("reborn column must get a fresh nonzero id, old=%d new=%d", oldID, newCol.ID)
	}
	if newCol.Type != TypeString {
		t.Fatalf("reborn column type must be string, got %s", newCol.Type)
	}

	// 新身份写入文本标签
	if err := s.Append(ctx, Row{"id": int64(3), "status": "hot", "note": "new-tag"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Row{"id": int64(4), "status": "warm", "note": "new-tag2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 1) 不带列裁剪的全列读：整表必须可读（不被旧 int64 字节搞挂），
	//    旧两行的新列值为 NULL。
	full := scanAll(t, s, ScanOptions{})
	gotStatus := []any{}
	for _, r := range full {
		gotStatus = append(gotStatus, r["status"])
	}
	logCase(t, biz, "全列读：旧行新列为NULL, 新行读到标签", "[nil nil hot warm]", gotStatus)
	if len(full) != 4 {
		t.Fatalf("want 4 rows, got %d", len(full))
	}
	if gotStatus[0] != nil || gotStatus[1] != nil || gotStatus[2] != "hot" || gotStatus[3] != "warm" {
		t.Fatalf("reborn read values wrong: %v", gotStatus)
	}

	// 2) 带列裁剪只投影重生列：结果必须与全列读完全一致
	projected := scanAll(t, s, ScanOptions{Columns: []string{"id", "status"}})
	pjStatus := []any{}
	for _, r := range projected {
		pjStatus = append(pjStatus, r["status"])
	}
	logCase(t, biz, "列裁剪读id,status：与全列读一致", "[nil nil hot warm]", pjStatus)
	if !anySliceEq(pjStatus, gotStatus) {
		t.Fatalf("pruned read disagrees with full read: %v vs %v", pjStatus, gotStatus)
	}

	// 3) 在重生列上带字符串过滤：只能命中新行，旧字节绝不能被当成标签匹配
	eq := scanAll(t, s, ScanOptions{
		Filters: []Filter{{Column: "status", Op: OpEqual, Value: "hot"}},
	})
	logCase(t, biz, "过滤status=='hot'只命新行", "id=3", idsOf(eq))
	if !int64SliceEq(idsOf(eq), []int64{3}) {
		t.Fatalf("reborn column filter must not read old bytes, got %v", idsOf(eq))
	}
	// 旧数值 909 的字节绝不可能以任何方式出现在结果里
	for _, r := range full {
		if r["status"] == int64(909) || r["status"] == "909" {
			t.Fatalf("old int64 value leaked through reborn column: %v", r)
		}
	}

	// 4) 合并后重生语义保持（新段按当前结构写；旧段旧身份帧丢弃）
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after := scanAll(t, s, ScanOptions{})
	afterStatus := []any{}
	for _, r := range after {
		afterStatus = append(afterStatus, r["status"])
	}
	logCase(t, biz, "合并后重生列旧行仍为NULL", "[nil nil hot warm]", afterStatus)
	if !anySliceEq(afterStatus, []any{nil, nil, "hot", "warm"}) {
		t.Fatalf("post-compaction reborn semantics broken: %v", afterStatus)
	}

	// 5) 重开后一致
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	reopened := scanAll(t, s2, ScanOptions{Columns: []string{"status"}})
	ro := []any{}
	for _, r := range reopened {
		ro = append(ro, r["status"])
	}
	logCase(t, biz, "重开后重生列语义保持", "[nil nil hot warm]", ro)
	if !anySliceEq(ro, []any{nil, nil, "hot", "warm"}) {
		t.Fatalf("reopen reborn semantics broken: %v", ro)
	}
	_ = s2.Close()
}

// TestRenameValidation 改名的边界规则。
func TestRenameValidation(t *testing.T) {
	biz := "列生命周期-改名校验"
	s := mustCreate(t, t.TempDir(), identitySchema(), DefaultOptions())
	defer s.Close()
	ctx := context.Background()

	_, err := renameColumn(*s.m.currentSchema(), "ghost", "x")
	logCase(t, biz, "改不存在的列", "ghost", err)
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("want ErrColumnNotFound, got %v", err)
	}
	err = s.RenameColumn(ctx, "status", "")
	logCase(t, biz, "改成空名字", `""`, err)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}
	if err := s.Append(ctx, Row{"id": int64(1), "status": int64(1), "note": "n"}); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameColumn(ctx, "status", "note"); !errors.Is(err, ErrIncompatibleSchema) {
		logCase(t, biz, "改成已存在的名字", "status->note", err)
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}
	logCase(t, biz, "改成已存在的名字", "status->note", "ErrIncompatibleSchema")
	if err := s.RenameColumn(ctx, "status", "status"); err != nil {
		logCase(t, biz, "改成自己幂等", "status->status", err)
		t.Fatalf("rename to self must be a no-op, got %v", err)
	}
	logCase(t, biz, "改成自己幂等", "status->status", "no-op")
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
