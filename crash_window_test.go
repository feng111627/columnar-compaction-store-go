package columnar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCompactionCrashDataPublishedBeforeSwap 覆盖窗口一：
// 新合并段已经完整落盘并 rename 为正式文件名，但清单还没来得及交换时
// 进程被杀。重启后必须完整回到合并前（行数、段集合都与合并前一致），
// 已落盘的新段被当作无主段清理，绝不能出现重复行/半截数据。
func TestCompactionCrashDataPublishedBeforeSwap(t *testing.T) {
	biz := "合并崩溃恢复-数据已落盘清单未交换"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	ctx := context.Background()
	writeNFlush(t, s, 4)
	oldNames := fileNamesOf(s.m.Segments)
	nextID := s.m.NextSegmentID // 崩溃的新段会占用这个 id
	logCase(t, biz, "合并前状态", map[string]any{"segs": len(oldNames), "nextID": nextID}, oldNames)

	// 崩溃前的正确参照结果
	beforeRows := scanAll(t, s, ScanOptions{})

	s.SetFailPoint("compactFailAfterDataPublish", true)
	err := s.Compact(ctx)
	logCase(t, biz, "注入：新段rename后、清单交换前崩溃", "new seg on disk, manifest old", err)
	if err == nil {
		t.Fatal("expected injected crash error")
	}
	s.SetFailPoint("compactFailAfterDataPublish", false)

	// 崩溃现场：内存/磁盘清单仍指向 4 个旧段；新段文件已在磁盘上
	if s.SegmentCount() != 4 {
		t.Fatalf("after crash manifest must still reference 4 old segments, got %d", s.SegmentCount())
	}
	published := segmentFileName(nextID)
	if _, statErr := os.Stat(filepath.Join(dir, published)); os.IsNotExist(statErr) {
		t.Fatalf("new segment %s must already be on disk at crash point", published)
	}
	logCase(t, biz, "崩溃现场文件并存", map[string]any{"old": len(oldNames), "new": published}, "visible")

	// 崩溃后、重启前，活着的进程读取必须仍是合并前结果（新段不在清单里）
	crashRows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "崩溃后重启前读取（看不到无主新段）", "1..4", idsOf(crashRows))
	if !int64SliceEq(idsOf(crashRows), idsOf(beforeRows)) {
		t.Fatalf("reads between crash and reopen must equal pre-compaction, got %v", idsOf(crashRows))
	}

	// 重启：必须完整回到合并前
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 2})
	if s2.SegmentCount() != 4 {
		t.Fatalf("after reopen must roll back to 4 segments, got %d", s2.SegmentCount())
	}
	if _, statErr := os.Stat(filepath.Join(dir, published)); !os.IsNotExist(statErr) {
		t.Fatalf("published-but-uncommitted segment %s must be cleaned on reopen", published)
	}
	rows := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重启后回到合并前：4段、1..4无重复", oldNames, idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{1, 2, 3, 4}) {
		t.Fatalf("recovered rows wrong: %v", idsOf(rows))
	}
	// 存活段必须恰好是原来的 4 个旧段，一个不多一个不少
	live := fileNamesOf(s2.m.Segments)
	if !stringSliceEq(live, oldNames) {
		t.Fatalf("live segments after reopen differ from pre-compaction: %v vs %v", live, oldNames)
	}

	// 回到一致状态后重新合并必须干净地停在合并后
	if err := s2.Compact(ctx); err != nil {
		t.Fatalf("compaction after rollback recovery failed: %v", err)
	}
	rows = scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "恢复后重新合并成功", "1 seg, 1..4", idsOf(rows))
	if s2.SegmentCount() != 1 || !int64SliceEq(idsOf(rows), []int64{1, 2, 3, 4}) {
		t.Fatalf("post-recovery compaction wrong: segs=%d rows=%v", s2.SegmentCount(), idsOf(rows))
	}
	_ = s2.Close()
}

// TestCompactionCrashTmpBeforeRename 覆盖窗口一的前半个瞬间：
// 数据还在 .tmp 里（或 rename 前一刻）进程被杀。重启后 .tmp 被清理，
// 完整回到合并前。
func TestCompactionCrashTmpBeforeRename(t *testing.T) {
	biz := "合并崩溃恢复-临时段未提交"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	ctx := context.Background()
	writeNFlush(t, s, 3)
	oldNames := fileNamesOf(s.m.Segments)

	s.SetFailPoint("compactFailTmpFsynced", true) // 数据已 fsync 到 .tmp，rename 前"被杀"
	err := s.Compact(ctx)
	logCase(t, biz, "注入：tmp写完rename前崩溃", ".tmp present", err)
	if err == nil {
		t.Fatal("expected injected crash error")
	}
	s.SetFailPoint("compactFailTmpFsynced", false)

	var tmps []string
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			tmps = append(tmps, e.Name())
		}
	}
	if len(tmps) == 0 {
		t.Fatal("expected a .tmp file at crash point")
	}
	logCase(t, biz, "崩溃现场", tmps, "tmp left behind")

	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 2})
	entries, _ = os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("tmp file %s must be cleaned on reopen", e.Name())
		}
	}
	rows := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重启后完整回到合并前", oldNames, idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{1, 2, 3}) || !stringSliceEq(fileNamesOf(s2.m.Segments), oldNames) {
		t.Fatalf("recovery to pre-compaction state failed: rows=%v segs=%v", idsOf(rows), fileNamesOf(s2.m.Segments))
	}
	_ = s2.Close()
}

// TestCompactionCrashAfterSwapOldFilesRemain 覆盖窗口二：
// 清单已切成可见状态（新段对外可见），但旧段文件还没清理干净时崩溃。
// 重启后必须干净停在合并后：清单只引用新段，无主旧段被清理，行数不重不丢。
func TestCompactionCrashAfterSwapOldFilesRemain(t *testing.T) {
	biz := "合并崩溃恢复-已可见旧文件未清"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	ctx := context.Background()
	writeNFlush(t, s, 4)
	oldNames := fileNamesOf(s.m.Segments)

	s.SetFailPoint("compactFailAfterSwap", true)
	err := s.Compact(ctx)
	logCase(t, biz, "注入：清单交换后旧段删除前崩溃", "new visible, orphans remain", err)
	if err == nil {
		t.Fatal("expected injected crash error")
	}
	s.SetFailPoint("compactFailAfterSwap", false)

	// 崩溃现场：清单已只引用新段；旧段物理上还在
	if s.SegmentCount() != 1 {
		t.Fatalf("after swap crash manifest must reference 1 new segment, got %d", s.SegmentCount())
	}
	// 此时扫描读到的必须已经是"合并后"的完整结果，且不重复
	midRows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "崩溃现场读取已是合并后", "1..4 once", idsOf(midRows))
	if !int64SliceEq(idsOf(midRows), []int64{1, 2, 3, 4}) {
		t.Fatalf("post-swap reads must see post-compaction data, got %v", idsOf(midRows))
	}

	// 重启：干净停在合并后
	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 2})
	if s2.SegmentCount() != 1 {
		t.Fatalf("after reopen must stay at 1 merged segment, got %d", s2.SegmentCount())
	}
	for _, name := range oldNames {
		if _, statErr := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(statErr) {
			t.Fatalf("orphan old segment %s must be removed on reopen", name)
		}
	}
	rows := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "重启后干净停在合并后：1段1..4", map[string]any{"cleaned": len(oldNames)}, idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{1, 2, 3, 4}) {
		t.Fatalf("post-swap recovery rows wrong: %v", idsOf(rows))
	}
	_ = s2.Close()
}

// TestReadersNeverSeeCompactionIntermediate 并发读者在两个崩溃窗口里反复
// 扫描，任何一次都只能看到合并前或合并后的完整快照（恰好 1..N 各一次），
// 看不到无主新段、不会行数翻倍、不会读到半截数据。
func TestReadersNeverSeeCompactionIntermediate(t *testing.T) {
	biz := "合并崩溃恢复-窗口期间读者只见完整快照"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 1, CompactionMinSegments: 2})
	ctx := context.Background()
	const N = 20
	for i := 1; i <= N; i++ {
		if err := s.Append(ctx, Row{"id": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	wantSum := int64(N * (N + 1) / 2)

	var failures int64
	var wg sync.WaitGroup
	var observedMu sync.Mutex
	observed := map[string]int{"pre": 0, "post": 0} // 读者看到的段数形态

	// 读者持续扫，直到合并完成。
	stop := make(chan struct{})
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sc, err := s.Scan(ctx, ScanOptions{})
				if err != nil {
					atomic.AddInt64(&failures, 1)
					t.Errorf("scan open: %v", err)
					return
				}
				var n int
				var sum int64
				for {
					row, rerr := sc.Next()
					if errors.Is(rerr, errEOF()) {
						break
					}
					if rerr != nil {
						atomic.AddInt64(&failures, 1)
						t.Errorf("next: %v", rerr)
						break
					}
					n++
					sum += row["id"].(int64)
				}
				sc.Close()
				// 合法快照：行数与总和必须恰好是完整集合。
				if n != N || sum != wantSum {
					atomic.AddInt64(&failures, 1)
					t.Errorf("intermediate snapshot seen: n=%d sum=%d", n, sum)
				}
				switch {
				case n == N:
					observedMu.Lock()
					if s.SegmentCount() > 1 {
						observed["pre"]++
					} else {
						observed["post"]++
					}
					observedMu.Unlock()
				}
			}
		}()
	}

	// 第一轮：在"数据已落盘、清单未交换"崩溃，随后重启等价的现场恢复
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.SetFailPoint("compactFailAfterDataPublish", true)
		_ = s.Compact(ctx)
		s.SetFailPoint("compactFailAfterDataPublish", false)
		time.Sleep(time.Millisecond)
		// 同一进程内现场即一致（清单仍旧），直接重试一次成功合并
		if err := s.Compact(ctx); err != nil {
			atomic.AddInt64(&failures, 1)
			t.Errorf("retry compact: %v", err)
		}
		close(stop)
	}()
	wg.Wait()

	logCase(t, biz, "读者快照形态统计（pre=多段,post=单段,均为完整1..20）", "failures=0", observed)
	if failures != 0 {
		t.Fatalf("got %d failures during crash-window reads", failures)
	}
	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "最终合并后结果", "1..20", idsOf(rows))
	if !int64SliceEq(idsOf(rows), loopIDs(1, N)) {
		t.Fatalf("final rows wrong: %v", idsOf(rows))
	}
	_ = s.Close()
}

func stringSliceEq(a, b []string) bool {
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

func loopIDs(from, to int) []int64 {
	out := make([]int64, 0, to-from+1)
	for i := from; i <= to; i++ {
		out = append(out, int64(i))
	}
	return out
}
