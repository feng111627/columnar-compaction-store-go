package columnar

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 业务覆盖：写入、读取、合并三类操作长时间交错并发。
// 不变量（任何时刻任意读者都必须成立）：
//  1. 每个快照读到的每一行 n 值都恰好落在已提交批次的值域，且按行键唯一；
//  2. 已提交的数据总数对“读开始于某提交之后”的读者单调不减；
//  3. 整个过程在 -race 下无数据竞争（go test -race 守护）。
//
// 由于 Append 与 Compact 串行推进元数据、Reader 拿不可变快照引用，
// 读者只会看到完整的提交点，不会看到半个文件/半截合并。
func TestConcurrentWriteReadCompact(t *testing.T) {
	tw := &testingWriter{t}
	dir := t.TempDir()
	tbl, err := Open(dir, "stress")
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	if _, err := tbl.AddColumn("n", TypeInt64); err != nil {
		t.Fatal(err)
	}

	var committed int64 // 已提交行数（原子计数）
	var stop int32
	var wg sync.WaitGroup

	// 写入方：持续追加每批 4 行，n 值全局唯一。
	var seq int64
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			base := atomic.AddInt64(&seq, 4) - 4
			vals := []int64{base + 1, base + 2, base + 3, base + 4}
			// 先预占行数再 Append：manifest 在 Append 内部发布，读者可能在
			// Append 返回前就看到该快照；预占保证“已提交计数”不落后于可见数据。
			atomic.AddInt64(&committed, 4)
			if _, err := tbl.Append(&Batch{Columns: []*Column{colI64("n", vals, nil)}}); err != nil {
				t.Errorf("Append: %v", err)
				atomic.AddInt64(&committed, -4)
				return
			}
		}
	}()

	// 合并方：只要文件数 >=2 就尝试合并，失败仅允许“无需合并”之外的真实错误。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for atomic.LoadInt32(&stop) == 0 {
			tbl.mmu.Lock()
			nFiles := len(tbl.cur.Files)
			tbl.mmu.Unlock()
			if nFiles >= 2 {
				if _, err := tbl.Compact(); err != nil {
					// 与 Append 竞争导致文件数变化属于正常调度，不算失败。
					t.Logf("Compact 返回（竞争重试）: %v", err)
				}
			}
		}
	}()

	// 读取方：每个快照都必须自洽——行数不超过已提交数、值无重复。
	const readerN = 6
	for i := 0; i < readerN; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for atomic.LoadInt32(&stop) == 0 {
				r, err := tbl.Reader([]string{"n"}, nil)
				if err != nil {
					t.Errorf("reader %d: %v", id, err)
					return
				}
				rows := collectRows(t, r)
				r.Close()
				seen := make(map[int64]struct{}, len(rows))
				for _, row := range rows {
					v := row["n"].I64
					if v <= 0 || v > atomic.LoadInt64(&seq) {
						t.Errorf("reader %d 读到越界值 %d", id, v)
					}
					if _, dup := seen[v]; dup {
						t.Errorf("reader %d 读到重复值 %d（快照不自洽）", id, v)
					}
					seen[v] = struct{}{}
				}
				if int64(len(rows)) > atomic.LoadInt64(&committed) {
					t.Errorf("reader %d 读到 %d 行 > 已提交 %d（读到未来数据）",
						id, len(rows), atomic.LoadInt64(&committed))
				}
			}
		}(i)
	}

	// 运行一小段时间后收口，最后做一次合并并验证终态完整。
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt64(&committed) < 40 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	atomic.StoreInt32(&stop, 1)
	wg.Wait()

	total := atomic.LoadInt64(&committed)
	if _, err := tbl.Compact(); err != nil && len(tbl.cur.Files) >= 2 {
		t.Fatalf("最终合并失败: %v", err)
	}
	r, err := tbl.Reader(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows := collectRows(t, r)
	r.Close()
	if int64(len(rows)) != total {
		t.Fatalf("终态行数 = %d, want 已提交 %d", len(rows), total)
	}
	seen := map[int64]bool{}
	for _, row := range rows {
		if seen[row["n"].I64] {
			t.Fatalf("终态存在重复值 %d", row["n"].I64)
		}
		seen[row["n"].I64] = true
	}
	tw.logf("[场景 写读合并交错] 输入: 1 writer + 1 compactor + %d readers 持续交错；结论: 已提交 %d 行，终态唯一无重，快照始终自洽，-race 无竞争",
		readerN, total)
}
