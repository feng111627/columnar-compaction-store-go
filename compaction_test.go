package columnar

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func openNumTable(t *testing.T) (*Table, string) {
	t.Helper()
	dir := t.TempDir()
	tbl, err := Open(dir, "nums")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := tbl.AddColumn("n", TypeInt64); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	if _, err := tbl.AddColumn("tag", TypeString); err != nil {
		t.Fatalf("AddColumn: %v", err)
	}
	return tbl, dir
}

func listDataFiles(t *testing.T, dir string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "tables", "nums", "data", "*.ccol"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(matches)
	return matches
}

// 业务覆盖：多个小文件合并成更大文件；行数、列值、顺序与合并前一致；
// 合并后旧文件被清理，只剩一个新文件。
func TestCompactBasic(t *testing.T) {
	tw := &testingWriter{t}
	tbl, dir := openNumTable(t)
	defer tbl.Close()

	var wantN []int64
	var wantTag []string
	for b := 0; b < 3; b++ {
		ns := []int64{int64(b*10 + 1), int64(b*10 + 2)}
		tags := []string{fmt.Sprintf("t%d-a", b), fmt.Sprintf("t%d-b", b)}
		if _, err := tbl.Append(&Batch{Columns: []*Column{
			colI64("n", ns, nil), colStr("tag", tags, nil),
		}}); err != nil {
			t.Fatalf("Append %d: %v", b, err)
		}
		wantN = append(wantN, ns...)
		wantTag = append(wantTag, tags...)
	}
	if fs := listDataFiles(t, dir); len(fs) != 3 {
		t.Fatalf("合并前应有 3 个数据文件, got %d", len(fs))
	}
	tw.logf("[场景 基础合并] 输入: 3 个小文件各 2 行, n=%v", wantN)

	stats, err := tbl.Compact()
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if stats.InputFiles != 3 || stats.RowsOut != 6 {
		t.Fatalf("合并统计异常: %+v", stats)
	}
	if fs := listDataFiles(t, dir); len(fs) != 1 {
		t.Fatalf("合并后应只剩 1 个数据文件, got %d: %v", len(fs), fs)
	}
	r, err := tbl.Reader([]string{"n", "tag"}, nil)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rows := collectRows(t, r)
	r.Close()
	if len(rows) != 6 {
		t.Fatalf("合并后行数 = %d, want 6", len(rows))
	}
	for i := range rows {
		if rows[i]["n"].I64 != wantN[i] || rows[i]["tag"].Str != wantTag[i] {
			t.Fatalf("第 %d 行合并后错乱: got (%d,%s) want (%d,%s)",
				i, rows[i]["n"].I64, rows[i]["tag"].Str, wantN[i], wantTag[i])
		}
	}
	tw.logf("[场景 基础合并] 结论: 3->1 个文件, 6 行顺序与取值完全一致 n=%v tag=%v", wantN, wantTag)

	// 持久化验证：重新打开仍只看到合并后的一个文件。
	tbl2, err := Open(dir, "nums")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer tbl2.Close()
	r2, err := tbl2.Reader(nil, nil)
	if err != nil {
		t.Fatalf("Reader reopen: %v", err)
	}
	if rows2 := collectRows(t, r2); len(rows2) != 6 {
		t.Fatalf("重开后行数 = %d, want 6", len(rows2))
	}
	r2.Close()
	tw.logf("[场景 基础合并] 结论: 重开表后从 manifest 恢复, 仍读到 6 行")
}

// 业务覆盖：合并过程中并发读取。由于换入窗口会等待所有在途读取结束、
// 且暂停新读取开始，读者必须看到“合并前全集”或“合并后全集”之一，
// 绝不能看到半截结果（行数必须恒为 N 或 0 缺失）。
func TestCompactConcurrentReaders(t *testing.T) {
	tw := &testingWriter{t}
	tbl, _ := openNumTable(t)
	defer tbl.Close()

	const batches = 8
	var totalRows int
	for b := 0; b < batches; b++ {
		ns := []int64{int64(b*100 + 1), int64(b*100 + 2), int64(b*100 + 3)}
		tags := []string{"a", "b", "c"}
		if _, err := tbl.Append(&Batch{Columns: []*Column{
			colI64("n", ns, nil), colStr("tag", tags, nil),
		}}); err != nil {
			t.Fatal(err)
		}
		totalRows += len(ns)
	}
	tw.logf("[场景 并发读+合并] 输入: %d 个小文件共 %d 行；并发 16 个读者 + 1 个合并", batches, totalRows)

	var wg sync.WaitGroup
	const readers = 16
	var observed []int
	var mu sync.Mutex
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r, err := tbl.Reader([]string{"n"}, nil)
			if err != nil {
				t.Errorf("reader %d: %v", id, err)
				return
			}
			defer r.Close()
			rows := collectRows(t, r)
			mu.Lock()
			observed = append(observed, len(rows))
			mu.Unlock()
			// 每个读者的快照必须自洽：n 集合恰好是写入值集合，无重无漏。
			seen := map[int64]bool{}
			for _, row := range rows {
				v := row["n"].I64
				if seen[v] {
					t.Errorf("reader %d 看到重复值 %d（合并中间态泄漏）", id, v)
				}
				seen[v] = true
			}
			if len(seen) != totalRows {
				t.Errorf("reader %d 看到 %d 个唯一值, want %d（快照不一致）", id, len(seen), totalRows)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := tbl.Compact(); err != nil {
			t.Errorf("Compact: %v", err)
		}
	}()
	wg.Wait()

	for i, n := range observed {
		if n != totalRows {
			t.Fatalf("读者 %d 观察到 %d 行, want %d（看到了合并中间态）", i, n, totalRows)
		}
	}
	tw.logf("[场景 并发读+合并] 结论: %d 个读者全部读到完整 %d 行快照，无重复/缺失/半截结果",
		len(observed), totalRows)
}

// 业务覆盖：合并中途失败（新文件已写出但 manifest 换入前失败）。
// 期望：旧 manifest 与旧文件全部保留，数据完整可读；新半成品被清理。
func TestCompactFailureRollback(t *testing.T) {
	tw := &testingWriter{t}
	tbl, dir := openNumTable(t)
	defer tbl.Close()

	for b := 0; b < 3; b++ {
		if _, err := tbl.Append(&Batch{Columns: []*Column{
			colI64("n", []int64{int64(b + 1)}, nil),
			colStr("tag", []string{fmt.Sprintf("g%d", b)}, nil),
		}}); err != nil {
			t.Fatal(err)
		}
	}
	before := listDataFiles(t, dir)
	tw.logf("[场景 合并失败回滚] 输入: 3 个旧文件 %v；注入 failBeforeSwap", before)

	tbl.setFailBeforeSwap(true)
	_, err := tbl.Compact()
	if err == nil {
		t.Fatal("注入失败后 Compact 必须返回错误")
	}
	tw.logf("[场景 合并失败回滚] 结论(1): Compact 按预期返回错误: %v", err)

	tbl.setFailBeforeSwap(false)
	after := listDataFiles(t, dir)
	if len(after) != len(before) {
		t.Fatalf("回滚后文件数 = %d, want %d（旧文件不应被删）", len(after), len(before))
	}
	for i := range before {
		if before[i] != after[i] {
			t.Fatalf("回滚后文件集合发生变化:\nbefore=%v\nafter =%v", before, after)
		}
	}
	// 不应留下任何 .tmp 半成品或孤儿数据文件。
	if leftovers, _ := filepath.Glob(filepath.Join(dir, "tables", "nums", "data", ".tmp-*")); len(leftovers) != 0 {
		t.Fatalf("回滚后残留临时文件: %v", leftovers)
	}
	if fs := listDataFiles(t, dir); len(fs) != 3 {
		t.Fatalf("回滚后仍应只有原 3 个数据文件, got %v", fs)
	}
	r, err := tbl.Reader(nil, nil)
	if err != nil {
		t.Fatalf("回滚后读取失败: %v", err)
	}
	rows := collectRows(t, r)
	r.Close()
	if len(rows) != 3 {
		t.Fatalf("回滚后行数 = %d, want 3", len(rows))
	}
	tw.logf("[场景 合并失败回滚] 结论(2): 旧 3 文件完整保留，读到 3 行；无 .tmp/孤儿文件；随后可重新合并")

	// 回滚后状态一致，可正常重新合并。
	stats, err := tbl.Compact()
	if err != nil {
		t.Fatalf("回滚后重新合并失败: %v", err)
	}
	if stats.RowsOut != 3 {
		t.Fatalf("重新合并行数 = %d, want 3", stats.RowsOut)
	}
	if fs := listDataFiles(t, dir); len(fs) != 1 {
		t.Fatalf("重新合并后应剩 1 个文件, got %v", fs)
	}
	tw.logf("[场景 合并失败回滚] 结论(3): 重新合并成功，3->1 文件，状态恢复一致")
}

// 业务覆盖：合并输入中存在损坏文件时，合并必须失败并原样保留合并前状态，
// 不能产出缺行的合并文件。
func TestCompactCorruptInputAborts(t *testing.T) {
	tbl, dir := openNumTable(t)
	defer tbl.Close()
	for b := 0; b < 2; b++ {
		if _, err := tbl.Append(&Batch{Columns: []*Column{
			colI64("n", []int64{int64(b + 1)}, nil),
			colStr("tag", []string{"x"}, nil),
		}}); err != nil {
			t.Fatal(err)
		}
	}
	before := listDataFiles(t, dir)
	// 破坏第二个文件的若干字节（避开魔数，命中 payload/校验）。
	target := before[1]
	st, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	pos := st.Size() - 30
	if err := writeByteAt(target, pos, 0xAB); err != nil {
		t.Fatal(err)
	}
	_ = dir
	_, err = tbl.Compact()
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("损坏输入合并应报 ErrCorruptData, got %v", err)
	}
	if fs := listDataFiles(t, dir); len(fs) != 2 {
		t.Fatalf("损坏中止后应保留原文件, got %v", fs)
	}
}
