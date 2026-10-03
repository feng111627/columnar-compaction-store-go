package columnar

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// rangedSchema 跳段测试用的两列结构：id 主键 + v 派生值（v = id*10）。
func rangedSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "v", Type: TypeInt64, Nullable: true},
	}}
}

// buildRangedSegments 按给定 id 区间逐段写入（每段 [lo,hi]，v=id*10），返回各段文件名。
func buildRangedSegments(t *testing.T, s *Store, ranges [][2]int64) []string {
	t.Helper()
	ctx := context.Background()
	for _, rg := range ranges {
		for id := rg[0]; id <= rg[1]; id++ {
			if err := s.Append(ctx, Row{"id": id, "v": id * 10}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return fileNamesOf(s.m.Segments)
}

// manualFilter 对全量扫描结果在内存里逐行求值过滤，作为"不跳段"的对照答案。
func manualFilter(t *testing.T, rows []Row, fs []Filter) []Row {
	t.Helper()
	var out []Row
	for _, r := range rows {
		keep, err := filtersMatch(r, fs)
		if err != nil {
			t.Fatal(err)
		}
		if keep {
			out = append(out, r)
		}
	}
	return out
}

func rowsEq(a, b []Row, cols ...string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		for _, c := range cols {
			if a[i][c] != b[i][c] {
				return false
			}
		}
	}
	return true
}

// TestSegmentSkipFiltersMatrix 结果正确性矩阵：各种过滤条件下，带跳段的扫描
// 结果必须与"全量扫描+内存逐行过滤"完全一致（不该跳的不跳，结果一行不少）。
func TestSegmentSkipFiltersMatrix(t *testing.T) {
	biz := "段级跳过-结果不比不跳时少"
	s := mustCreate(t, t.TempDir(), rangedSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s.Close()
	buildRangedSegments(t, s, [][2]int64{{1, 10}, {11, 20}, {21, 30}, {31, 40}})

	full := scanAll(t, s, ScanOptions{})
	filters := []Filter{
		{Column: "id", Op: OpEqual, Value: int64(15)},
		{Column: "id", Op: OpEqual, Value: int64(100)},  // 超出全部段：整表跳过
		{Column: "id", Op: OpNotEqual, Value: int64(5)}, // 只有 seg1 的 min==max!=5 不成立，都不能跳
		{Column: "id", Op: OpLess, Value: int64(1)},     // 空结果
		{Column: "id", Op: OpLessEqual, Value: int64(10)},
		{Column: "id", Op: OpGreater, Value: int64(40)}, // 空结果
		{Column: "id", Op: OpGreaterEqual, Value: int64(35)},
		{Column: "v", Op: OpEqual, Value: int64(200)}, // 派生列上跳过
		{Column: "v", Op: OpGreater, Value: int64(205)},
	}
	for _, f := range filters {
		got := scanAll(t, s, ScanOptions{Filters: []Filter{f}})
		want := manualFilter(t, full, []Filter{f})
		logCase(t, biz, fmt.Sprintf("id %s %v", f.Op, f.Value), "4段x10行", idsOf(got))
		if !rowsEq(got, want, "id", "v") {
			t.Fatalf("filter %v: got %v want %v", f, idsOf(got), idsOf(want))
		}
		// 列裁剪 + 过滤 + 跳段三者叠加，结果同样一致
		gotPruned := scanAll(t, s, ScanOptions{Columns: []string{"id"}, Filters: []Filter{f}})
		if !rowsEq(gotPruned, want, "id") {
			t.Fatalf("pruned filter %v: got %v want %v", f, idsOf(gotPruned), idsOf(want))
		}
	}
}

// TestSegmentSkipActuallySkips 用"损坏未被跳过的段会报错、被跳过的段不报错"
// 证明跳过真实发生：命中范围外的段即使数据帧物理损坏也不影响过滤读。
func TestSegmentSkipActuallySkips(t *testing.T) {
	biz := "段级跳过-该跳的真的跳"
	dir := t.TempDir()
	s := mustCreate(t, dir, rangedSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	names := buildRangedSegments(t, s, [][2]int64{{1, 10}, {11, 20}, {21, 30}, {31, 40}})

	// 物理损坏前三个段的数据帧（文件尾部属于最后一个列帧）
	for _, n := range names[:3] {
		p := filepath.Join(dir, n)
		mutateFile(t, p, func(b []byte) []byte {
			b[len(b)-2] ^= 0xFF
			return b
		})
	}
	logCase(t, biz, "损坏前3段的数据帧", "seg1..3 frame CRC broken", "全列扫描应报损坏")

	// 全量扫描：必须报损坏（证明损坏确实在读取路径上）
	sc, err := s.Scan(context.Background(), ScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, scanErr := drain(sc)
	logCase(t, biz, "全列扫描(无过滤)", "读到损坏段", scanErr)
	if !errors.Is(scanErr, ErrCorruptData) {
		t.Fatalf("full scan must hit corruption, got %v", scanErr)
	}

	// 过滤 id>=35：前三段被统计块跳过，损坏帧根本不会被读
	rows := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(35)}}})
	logCase(t, biz, "过滤 id>=35", "前3段损坏但被跳过", idsOf(rows))
	if !int64SliceEq(idsOf(rows), []int64{35, 36, 37, 38, 39, 40}) {
		t.Fatalf("skipped scan wrong: %v", idsOf(rows))
	}

	// 过滤 id<=5：只有 seg1 可能命中，但 seg1 已损坏 -> 必须报错（不该跳的不跳）
	sc2, err := s.Scan(context.Background(), ScanOptions{Filters: []Filter{{Column: "id", Op: OpLessEqual, Value: int64(5)}}})
	if err != nil {
		t.Fatal(err)
	}
	_, scanErr2 := drain(sc2)
	logCase(t, biz, "过滤 id<=5 (命中损坏的seg1)", "seg1 不该跳", scanErr2)
	if !errors.Is(scanErr2, ErrCorruptData) {
		t.Fatalf("segment in filter range must be read (and fail), got %v", scanErr2)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func drain(sc Scanner) ([]Row, error) {
	defer sc.Close()
	var rows []Row
	for {
		r, err := sc.Next()
		if errors.Is(err, errEOF()) {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, r)
	}
}

// TestSegmentSkipStatsCorruptFallback 统计块损坏时安全回退完整扫描：
// 结果必须与统计完好时完全一致，且不允许漏行或报错。
func TestSegmentSkipStatsCorruptFallback(t *testing.T) {
	biz := "段级跳过-统计损坏回退全扫"
	dir := t.TempDir()
	s := mustCreate(t, dir, rangedSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	names := buildRangedSegments(t, s, [][2]int64{{1, 10}, {11, 20}, {21, 30}, {31, 40}})
	f := Filter{Column: "id", Op: OpGreaterEqual, Value: int64(25)}
	want := idsOf(scanAll(t, s, ScanOptions{Filters: []Filter{f}}))
	logCase(t, biz, "统计完好时的过滤结果", "id>=25", want)

	// 破坏所有段的统计块 payload（固定头 16B + statsLen 4B 之后），CRC 校验失败
	for _, n := range names {
		mutateFile(t, filepath.Join(dir, n), func(b []byte) []byte {
			b[20] ^= 0xFF
			return b
		})
	}
	got := scanAll(t, s, ScanOptions{Filters: []Filter{f}})
	logCase(t, biz, "统计块损坏后的过滤结果", "stats CRC broken x4", idsOf(got))
	if !int64SliceEq(idsOf(got), want) {
		t.Fatalf("fallback scan must equal stats-guided scan: got %v want %v", idsOf(got), want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSegmentSkipAfterCompaction 合并重写段文件时统计随数据重新计算，
// 合并前后过滤结果一致，且合并后的段依然能跳。
func TestSegmentSkipAfterCompaction(t *testing.T) {
	biz := "段级跳过-合并后统计一致"
	dir := t.TempDir()
	s := mustCreate(t, dir, rangedSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 2})
	buildRangedSegments(t, s, [][2]int64{{1, 10}, {11, 20}, {21, 30}, {31, 40}})
	ctx := context.Background()

	f := Filter{Column: "id", Op: OpGreaterEqual, Value: int64(25)}
	before := idsOf(scanAll(t, s, ScanOptions{Filters: []Filter{f}}))
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after := idsOf(scanAll(t, s, ScanOptions{Filters: []Filter{f}}))
	logCase(t, biz, "合并前后过滤结果", "id>=25, 4段->1段", []any{before, after})
	if !int64SliceEq(before, after) {
		t.Fatalf("compaction changed filtered results: %v -> %v", before, after)
	}

	// 合并后的单段统计为 [1,40]：过滤 id>=100 整段可跳。
	// 损坏合并段的数据帧后，该过滤读不受影响（证明合并段的统计块生效）。
	names := fileNamesOf(s.m.Segments)
	if len(names) != 1 {
		t.Fatalf("want 1 segment after compaction, got %d", len(names))
	}
	mutateFile(t, filepath.Join(dir, names[0]), func(b []byte) []byte {
		b[len(b)-2] ^= 0xFF
		return b
	})
	rows := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(100)}}})
	logCase(t, biz, "合并段损坏+过滤 id>=100", "整段跳过", idsOf(rows))
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %v", idsOf(rows))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSegmentSkipWithRename 改名后旧段的统计块（记录的是老名字）仍通过
// 列身份映射生效：过滤新名字时旧段照样能被跳过。
func TestSegmentSkipWithRename(t *testing.T) {
	biz := "段级跳过-改名后统计仍生效"
	dir := t.TempDir()
	s := mustCreate(t, dir, rangedSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	ctx := context.Background()
	buildRangedSegments(t, s, [][2]int64{{1, 10}}) // seg1: 统计记在老名字 v 下
	if err := s.RenameColumn(ctx, "v", "w"); err != nil {
		t.Fatal(err)
	}
	// seg2: 改名后写入，统计记在新名字 w 下
	for id := int64(11); id <= 20; id++ {
		if err := s.Append(ctx, Row{"id": id, "w": id * 10}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 损坏 seg1 的数据帧；过滤 w>=1000 超出两段统计范围（seg1 经身份映射
	// 找到老名字 v 下的统计 [10,100]，seg2 统计 [110,200]），都被跳过。
	seg1 := fileNamesOf(s.m.Segments)[0]
	mutateFile(t, filepath.Join(dir, seg1), func(b []byte) []byte {
		b[len(b)-2] ^= 0xFF
		return b
	})
	rows := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "w", Op: OpGreaterEqual, Value: int64(1000)}}})
	logCase(t, biz, "过滤 w>=1000 (seg1损坏但被跳过)", "rename v->w", idsOf(rows))
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %v", idsOf(rows))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSegmentSkipByteLevel 字节级证明：被跳过的段一个数据帧都不读，
// 只读固定头 + 统计块 + 列目录。
func TestSegmentSkipByteLevel(t *testing.T) {
	biz := "段级跳过-字节级零帧读取"
	_, segPath := buildOneSegmentStore(t)
	raw := readFileBytes(t, segPath)
	schema := testSchema2()
	meta := segmentMeta{FileName: "inmem", RowCount: 2}

	_, h, err := openDecoder(&sliceReaderAt{raw}, int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	frameEnd := h.dir[len(h.dir)-1].offset + int64(h.dir[len(h.dir)-1].frameLen)

	// id 取值 [1,2]；过滤 id>=100 必然无命中 -> 整段跳过
	cr := &countingReaderAt{data: raw}
	rd, err := openSegmentReaderAt(cr, int64(len(raw)), meta, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := rd.scan(context.Background(), ScanOptions{
		Filters: []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(100)}},
	})
	_ = rd.close()
	if err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "过滤 id>=100 (段内id为1..2)", "2行段", map[string]any{
		"rows": len(rows), "readBytes": cr.bytes, "fileBytes": len(raw),
	})
	if len(rows) != 0 {
		t.Fatalf("want 0 rows, got %d", len(rows))
	}
	for _, rg := range cr.ranges {
		if rg[1] > h.dataStart {
			t.Fatalf("skipped segment must not read any column frame, read range %v crosses data start %d (frame end %d)",
				rg, h.dataStart, frameEnd)
		}
	}

	// 对照：过滤 id>=1 可能命中 -> 必须读数据帧
	cr2 := &countingReaderAt{data: raw}
	rd2, err := openSegmentReaderAt(cr2, int64(len(raw)), meta, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	rows2, err := rd2.scan(context.Background(), ScanOptions{
		Filters: []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(1)}},
	})
	_ = rd2.close()
	if err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "对照: 过滤 id>=1", "不该跳", map[string]any{"rows": len(rows2), "readBytes": cr2.bytes})
	if len(rows2) != 2 || cr2.bytes <= cr.bytes {
		t.Fatalf("in-range filter must read frames: rows=%d bytes=%d (skipped=%d)",
			len(rows2), cr2.bytes, cr.bytes)
	}
}

// TestSegmentSkipAllNullColumn 全 NULL 列没有统计项：不能跳过（保守），
// 但过滤结果必须正确（NULL 不匹配任何比较）。
func TestSegmentSkipAllNullColumn(t *testing.T) {
	biz := "段级跳过-全NULL列无统计"
	s := mustCreate(t, t.TempDir(), Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "note", Type: TypeString, Nullable: true},
	}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s.Close()
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		if err := s.Append(ctx, Row{"id": int64(i)}); err != nil { // note 全 NULL
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rows := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "note", Op: OpEqual, Value: "x"}}})
	logCase(t, biz, "过滤 note=='x' (note全NULL)", "5行段", len(rows))
	if len(rows) != 0 {
		t.Fatalf("NULL never matches: want 0 rows, got %d", len(rows))
	}
}
