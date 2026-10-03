package columnar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func compactSchema() Schema {
	return Schema{Columns: []Column{{Name: "id", Type: TypeInt64, Nullable: false}}}
}

func writeNFlush(t *testing.T, s *Store, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 1; i <= n; i++ {
		if err := s.Append(ctx, Row{"id": int64(i)}); err != nil {
			t.Fatal(err)
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompactionBasicAndSelection(t *testing.T) {
	biz := "小文件合并-选段与结果正确"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 3})
	ctx := context.Background()
	writeNFlush(t, s, 5)

	segs := s.m.Segments
	got := pickCompactionSegments(s.m, s.opt)
	logCase(t, biz, "段数达到阈值全部选中", "5 segs, threshold 3", len(got))
	if len(got) != 5 {
		t.Fatalf("want 5 picked, got %d", len(got))
	}

	rows := scanAll(t, s, ScanOptions{})
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "合并后段数", "5 -> 1", s.SegmentCount())
	if s.SegmentCount() != 1 {
		t.Fatalf("want 1 segment, got %d", s.SegmentCount())
	}
	after := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "合并后行数与顺序", "1..5", idsOf(after))
	if len(after) != 5 || !int64SliceEq(idsOf(after), []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("data changed after compaction: %v", idsOf(after))
	}
	if len(rows) != 5 {
		t.Fatalf("pre-compaction read broken: %d", len(rows))
	}

	// 旧段文件必须已经物理删除
	for _, old := range segs[:len(segs)-0] {
		if _, err := os.Stat(filepath.Join(dir, old.FileName)); !os.IsNotExist(err) {
			t.Fatalf("old segment %s should have been removed, err=%v", old.FileName, err)
		}
	}

	// 单段再次合并应为 no-op
	got = pickCompactionSegments(s.m, s.opt)
	logCase(t, biz, "单段不再合并", "1 seg", len(got))
	if len(got) != 0 {
		t.Fatalf("single segment should not be compacted")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionSmallSegmentsByTargetRows(t *testing.T) {
	biz := "小文件合并-按目标行数选段"
	s := mustCreate(t, t.TempDir(), compactSchema(), Options{
		FlushThreshold: 100, CompactionMinSegments: 100, CompactionTargetRows: 50,
	})
	writeNFlush(t, s, 3)
	got := pickCompactionSegments(s.m, s.opt)
	logCase(t, biz, "3个小段(行数<50)", "3 segs", len(got))
	if len(got) != 3 {
		t.Fatalf("want 3 small segments picked, got %d", len(got))
	}
}

func TestCompactionRollbackBeforeSwap(t *testing.T) {
	biz := "小文件合并-交换前失败回滚"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	ctx := context.Background()
	writeNFlush(t, s, 4)
	before := s.SegmentCount()
	beforeRows := scanAll(t, s, ScanOptions{})

	s.SetFailPoint("compactFailBeforeSwap", true)
	err := s.Compact(ctx)
	logCase(t, biz, "注入交换前失败", "4 segs", err)
	if err == nil {
		t.Fatal("expected injected error")
	}
	s.SetFailPoint("compactFailBeforeSwap", false)

	logCase(t, biz, "失败后段数不变", before, s.SegmentCount())
	if s.SegmentCount() != before {
		t.Fatalf("segments changed after failed compaction: %d != %d", s.SegmentCount(), before)
	}
	afterRows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "失败后数据可读且完整", "1..4", idsOf(afterRows))
	if !int64SliceEq(idsOf(afterRows), idsOf(beforeRows)) {
		t.Fatalf("data changed after rollback: %v", idsOf(afterRows))
	}

	// 磁盘上不应残留临时文件，也不应残留新段
	entries, _ := os.ReadDir(dir)
	var leftover []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			leftover = append(leftover, e.Name())
		}
	}
	logCase(t, biz, "回滚后无临时文件", "dir scan", leftover)
	if len(leftover) != 0 {
		t.Fatalf("leftover temp files: %v", leftover)
	}

	// 重试合并应当成功（系统已回到一致状态）
	if err := s.Compact(ctx); err != nil {
		t.Fatalf("retry compaction failed: %v", err)
	}
	logCase(t, biz, "回滚后重试合并成功", "retry", s.SegmentCount())
	if s.SegmentCount() != 1 {
		t.Fatalf("want 1 segment after retry, got %d", s.SegmentCount())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCompactionCrashAfterSwapRecovery(t *testing.T) {
	biz := "小文件合并-交换后崩溃的恢复"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	ctx := context.Background()
	writeNFlush(t, s, 4)
	oldNames := fileNamesOf(s.m.Segments)

	s.SetFailPoint("compactFailAfterSwap", true)
	err := s.Compact(ctx)
	logCase(t, biz, "注入交换后崩溃", "manifest swapped, old files remain", err)
	if err == nil {
		t.Fatal("expected injected error")
	}
	// 此刻：清单已指向新段，旧段文件还在磁盘上
	if s.SegmentCount() != 1 {
		t.Fatalf("in-memory manifest should point to 1 segment, got %d", s.SegmentCount())
	}

	// 用新实例重新打开：清理无主旧段，数据完整可读
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 2})
	rows := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重启后数据完整", "1..4", idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{1, 2, 3, 4}) {
		t.Fatalf("recovered data wrong: %v", idsOf(rows))
	}
	entries, _ := os.ReadDir(dir)
	var remain []string
	for _, e := range entries {
		for _, n := range oldNames {
			if e.Name() == n {
				remain = append(remain, e.Name())
			}
		}
	}
	logCase(t, biz, "旧段文件已作为无主文件清理", len(oldNames), remain)
	if len(remain) != 0 {
		t.Fatalf("orphaned old segments not cleaned: %v", remain)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReopenKeepsData(t *testing.T) {
	biz := "小文件合并-持久化重开"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 2, CompactionMinSegments: 10})
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if err := s.Append(ctx, Row{"id": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2 := mustOpen(t, dir, Options{FlushThreshold: 2, CompactionMinSegments: 10})
	rows := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "关闭重开数据不丢", "1..5", idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{1, 2, 3, 4, 5}) {
		t.Fatalf("reopen data wrong: %v", idsOf(rows))
	}
	_ = s2.Close()
	_ = errors.Is
}
