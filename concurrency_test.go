package columnar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestConcurrentReadersDuringCompaction 验证合并期间的一致快照：
// 多个读者在合并开始前打开扫描器、持有过程中后台反复合并，
// 每个读者都必须看到一个"完整"的数据集（要么旧集合要么新集合，
// 行数与 id 集合等于某个合法快照），且读不报错（不会读到被删文件/半截段）。
func TestConcurrentReadersDuringCompaction(t *testing.T) {
	biz := "并发-合并期间一致快照"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 1, CompactionMinSegments: 2})
	ctx := context.Background()

	const N = 40
	for i := 1; i <= N; i++ {
		if err := s.Append(ctx, Row{"id": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("[业务=%s] 初始段数=%d（每行一段，最大化合并期间文件删除竞争）", biz, s.SegmentCount())

	var wg sync.WaitGroup
	var failures int64
	var seenCounts []int
	var countMu sync.Mutex

	// 读者：打开快照后稍作停留，让合并与其重叠。
	const readers = 12
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			sc, err := s.Scan(ctx, ScanOptions{})
			if err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("reader %d scan open: %v", id, err)
				return
			}
			time.Sleep(time.Duration(2+id%5) * time.Millisecond)
			var n int64
			sum := int64(0)
			for {
				row, err := sc.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					atomic.AddInt64(&failures, 1)
					t.Errorf("reader %d next: %v", id, err)
					break
				}
				n++
				sum += row["id"].(int64)
			}
			sc.Close()
			// 合法快照：看到全部 N 行（本测试不并发写），总和 1..N。
			wantSum := int64(N * (N + 1) / 2)
			if n != N || sum != wantSum {
				atomic.AddInt64(&failures, 1)
				t.Errorf("reader %d inconsistent snapshot: n=%d sum=%d want n=%d sum=%d",
					id, n, sum, N, wantSum)
			}
			countMu.Lock()
			seenCounts = append(seenCounts, int(n))
			countMu.Unlock()
		}(r)
	}

	// 合并者：多轮 compact；第一轮制造大段，之后为 no-op 也不允许出错。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for k := 0; k < 4; k++ {
			if err := s.Compact(ctx); err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("compact round %d: %v", k, err)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	logCase(t, biz, fmt.Sprintf("%d个读者在合并期间读到的行数", readers), "期望全部=40", seenCounts)
	if failures != 0 {
		t.Fatalf("got %d concurrent failures", failures)
	}
	// 合并完成后应当只剩一个段
	if c := s.SegmentCount(); c != 1 {
		t.Fatalf("want 1 segment after compactions, got %d", c)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentWritesAndReads 验证并发追加/合并/读取下数据不丢不重。
func TestConcurrentWritesAndReads(t *testing.T) {
	biz := "并发-读写合并混合"
	dir := t.TempDir()
	s := mustCreate(t, dir, compactSchema(), Options{FlushThreshold: 7, CompactionMinSegments: 3})
	ctx := context.Background()

	const writers, perWriter = 4, 30
	var written int64
	var failures int64

	var writerWg, readerWg, compWg sync.WaitGroup

	for w := 0; w < writers; w++ {
		writerWg.Add(1)
		go func() {
			defer writerWg.Done()
			for i := 0; i < perWriter; i++ {
				v := atomic.AddInt64(&written, 1)
				if err := s.Append(ctx, Row{"id": v}); err != nil {
					atomic.AddInt64(&failures, 1)
					t.Errorf("append: %v", err)
					return
				}
			}
		}()
	}

	stop := make(chan struct{})
	readerWg.Add(1)
	go func() {
		defer readerWg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			sc, err := s.Scan(ctx, ScanOptions{})
			if err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("scan: %v", err)
				return
			}
			n := 0
			for {
				_, err := sc.Next()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					atomic.AddInt64(&failures, 1)
					t.Errorf("next: %v", err)
					break
				}
				n++
			}
			sc.Close()
			if n < 0 {
				t.Errorf("negative snapshot size")
			}
		}
	}()

	compWg.Add(1)
	go func() {
		defer compWg.Done()
		for k := 0; k < 6; k++ {
			time.Sleep(2 * time.Millisecond)
			if err := s.Compact(ctx); err != nil {
				atomic.AddInt64(&failures, 1)
				t.Errorf("compact: %v", err)
				return
			}
		}
	}()

	writerWg.Wait()
	compWg.Wait()
	close(stop)
	readerWg.Wait()

	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "最终行数", writers*perWriter, len(rows))
	if len(rows) != writers*perWriter {
		t.Fatalf("lost or duplicated rows: got %d, want %d", len(rows), writers*perWriter)
	}
	uniq := map[int64]bool{}
	for _, r := range rows {
		v := r["id"].(int64)
		if uniq[v] {
			t.Fatalf("duplicate id %d", v)
		}
		uniq[v] = true
	}
	logCase(t, biz, "最终id唯一性", len(rows), len(uniq))
	if failures != 0 {
		t.Fatalf("concurrent failures: %d", failures)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
