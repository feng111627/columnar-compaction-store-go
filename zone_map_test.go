package columnar

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

func zoneSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "age", Type: TypeInt64, Nullable: true},
		{Name: "tag", Type: TypeString, Nullable: true},
		{Name: "score", Type: TypeFloat64, Nullable: true},
	}}
}

// scanWithPruning 打开扫描器、耗尽、返回结果行与段跳过统计。
func scanWithPruning(t *testing.T, s *Store, opts ScanOptions) ([]Row, int, int) {
	t.Helper()
	sc, err := s.Scan(context.Background(), opts)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	defer sc.Close()
	var rows []Row
	for {
		r, err := sc.Next()
		if errors.Is(err, errEOF()) {
			break
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		rows = append(rows, r)
	}
	considered, skipped := sc.(*snapshotScanner).PruningStats()
	return rows, considered, skipped
}

// buildZoneStore 建 4 个段，每段 age 区间互不重叠：
//
//	seg1 age[10,19] seg2 age[20,29] seg3 age[30,39] seg4 age[40,49]
func buildZoneStore(t *testing.T) *Store {
	t.Helper()
	s := mustCreate(t, t.TempDir(), zoneSchema(),
		Options{FlushThreshold: 100, CompactionMinSegments: 100})
	ctx := context.Background()
	for g := 0; g < 4; g++ {
		for k := 0; k < 5; k++ {
			age := int64(g*10 + 10 + k)
			if err := s.Append(ctx, Row{
				"id":    int64(g*5 + k + 1),
				"age":   age,
				"tag":   []string{"a", "b", "c", "d"}[g],
				"score": float64(age),
			}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// TestSegmentPruningSkipsWholeSegments 该跳的段整段跳，不该跳的不跳，
// 且结果与不过滤的全扫一致；列裁剪照常生效。
func TestSegmentPruningSkipsWholeSegments(t *testing.T) {
	biz := "段级跳过-该跳的跳不该跳的不跳"
	s := buildZoneStore(t)
	defer s.Close()

	// 无过滤：不跳过任何段
	all, considered, skipped := scanWithPruning(t, s, ScanOptions{})
	logCase(t, biz, "无过滤全扫", "4 segs, 20 rows", map[string]any{"considered": considered, "skipped": skipped, "rows": len(all)})
	if considered != 4 || skipped != 0 || len(all) != 20 {
		t.Fatalf("no-filter scan must touch all segments: considered=%d skipped=%d rows=%d", considered, skipped, len(all))
	}

	// age >= 40：只可能命中 seg4，其余 3 段整段跳过
	rows, considered, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(40)}},
	})
	got := idsOf(rows)
	logCase(t, biz, "age>=40 跳过3段", "ids 16..20, skipped=3", map[string]any{"ids": got, "considered": considered, "skipped": skipped})
	if skipped != 3 || considered != 4 {
		t.Fatalf("want 3 of 4 segments skipped, got considered=%d skipped=%d", considered, skipped)
	}
	if !int64SliceEq(got, []int64{16, 17, 18, 19, 20}) {
		t.Fatalf("age>=40 rows wrong: %v", got)
	}

	// age == 24：只有 seg2 可能命中，跳 3 段
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpEqual, Value: int64(24)}},
	})
	got = idsOf(rows)
	logCase(t, biz, "age==24 只留seg2", "id=10, skipped=3", map[string]any{"ids": got, "skipped": skipped})
	if skipped != 3 || !int64SliceEq(got, []int64{10}) {
		t.Fatalf("age==24 pruning wrong: ids=%v skipped=%d", got, skipped)
	}

	// age < 0：所有段都不可能命中，4 段全跳，结果为空
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpLess, Value: int64(0)}},
	})
	logCase(t, biz, "age<0 四段全跳", "0 rows, skipped=4", map[string]any{"rows": len(rows), "skipped": skipped})
	if skipped != 4 || len(rows) != 0 {
		t.Fatalf("impossible filter must skip all: rows=%d skipped=%d", len(rows), skipped)
	}

	// 跨整域的条件：一段都不能跳。24 确实存在于 seg2，!=24 命中其余 19 行。
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpNotEqual, Value: int64(24)}},
	})
	logCase(t, biz, "age!=24 不可排除任何段", "19 rows, skipped=0", map[string]any{"rows": len(rows), "skipped": skipped})
	if skipped != 0 || len(rows) != 19 {
		t.Fatalf("!= must not prune: rows=%d skipped=%d", len(rows), skipped)
	}

	// 字符串极值：tag == "c" 只有 seg3 可能
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "tag", Op: OpEqual, Value: "c"}},
	})
	got = idsOf(rows)
	logCase(t, biz, "tag=='c' 只留seg3", "ids 11..15", map[string]any{"ids": got, "skipped": skipped})
	if skipped != 3 || !int64SliceEq(got, []int64{11, 12, 13, 14, 15}) {
		t.Fatalf("string pruning wrong: ids=%v skipped=%d", got, skipped)
	}

	// 列裁剪 + 跳过组合：只投影 tag，仍正确跳过且结果不含其它列
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Columns: []string{"tag"},
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(40)}},
	})
	logCase(t, biz, "投影tag + age>=40", "5 rows, only tag key", map[string]any{"rows": len(rows), "skipped": skipped, "first": rows[0]})
	if skipped != 3 || len(rows) != 5 {
		t.Fatalf("project+prune rows/skips wrong: %d %d", len(rows), skipped)
	}
	for _, r := range rows {
		if len(r) != 1 {
			t.Fatalf("projected row must have only tag, got %v", r)
		}
		if _, ok := r["age"]; ok {
			t.Fatalf("filter column age must not be projected: %v", r)
		}
	}

	// 通用正确性：多种阈值下，跳过扫描结果必须与完整扫描（关闭跳过逻辑）
	// 得到的结果完全一致——"结果不比不跳时少"，也不多。
	for _, v := range []int64{5, 10, 15, 19, 20, 35, 40, 49, 50} {
		pruned, _, _ := scanWithPruning(t, s, ScanOptions{
			Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: v}},
		})
		full := fullScanWithoutPruning(t, s, Filter{Column: "age", Op: OpGreaterEqual, Value: v})
		if !int64SliceEq(idsOf(pruned), idsOf(full)) {
			t.Fatalf("age>=%d pruned result %v != full result %v", v, idsOf(pruned), idsOf(full))
		}
	}
	logCase(t, biz, "9个阈值：跳过扫描与完整扫描结果逐一相等", "v in {5..50}", "identical")
}

// fullScanWithoutPruning 直接对底层段逐个完整扫描（不经段级跳过），作为
// "如果不跳会得到什么"的参照。
func fullScanWithoutPruning(t *testing.T, s *Store, f Filter) []Row {
	t.Helper()
	s.mu.RLock()
	metas := make([]segmentMeta, len(s.m.Segments))
	copy(metas, s.m.Segments)
	schema := *s.m.currentSchema()
	hist := append([]Schema(nil), s.m.SchemaHistory...)
	s.mu.RUnlock()

	var out []Row
	for _, meta := range metas {
		ws, ok := lookupSchema(hist, meta.SchemaVersion)
		if !ok {
			t.Fatalf("schema version %d missing", meta.SchemaVersion)
		}
		rd, err := openSegment(s.dir, meta, ws, schema)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := rd.scan(context.Background(), ScanOptions{Filters: []Filter{f}})
		_ = rd.close()
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, rows...)
	}
	return out
}

func lookupSchema(hist []Schema, v int) (Schema, bool) {
	for _, h := range hist {
		if h.Version == v {
			return h, true
		}
	}
	return Schema{}, false
}

// TestSegmentPruningAfterCompaction 合并后依据随数据重算，跳过仍正确且
// 结果与合并前一致。
func TestSegmentPruningAfterCompaction(t *testing.T) {
	biz := "段级跳过-合并后依据一致"
	s := buildZoneStore(t)
	defer s.Close()
	ctx := context.Background()

	before, _, _ := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(30)}},
	})

	// 把 CompactionMinSegments 调小到触发全量合并（直接改内存选项）
	s.opt.CompactionMinSegments = 4
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	if s.SegmentCount() != 1 {
		t.Fatalf("want 1 merged segment, got %d", s.SegmentCount())
	}
	logCase(t, biz, "4段合并为1段并重算依据", "1 seg", s.m.Segments[0].Stats["age"])

	// age>=30 在合并段上：不跳过（段内确有 [30,49]），结果与合并前相同
	after, considered, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(30)}},
	})
	logCase(t, biz, "合并后 age>=30", "ids 11..20", map[string]any{"skipped": skipped, "considered": considered})
	if !int64SliceEq(idsOf(after), idsOf(before)) {
		t.Fatalf("post-compaction pruning result %v != %v", idsOf(after), idsOf(before))
	}

	// age>=100：合并段依据 [10,49]，整段跳过，结果为空
	none, _, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(100)}},
	})
	logCase(t, biz, "合并后 age>=100 整段跳过", "0 rows", map[string]any{"rows": len(none), "skipped": skipped})
	if len(none) != 0 || skipped != 1 {
		t.Fatalf("merged segment should be pruned, rows=%d skipped=%d", len(none), skipped)
	}
}

// TestSegmentPruningNullAndMixed 全 NULL / 部分 NULL 段的跳过语义。
func TestSegmentPruningNullAndMixed(t *testing.T) {
	biz := "段级跳过-NULL语义"
	s := mustCreate(t, t.TempDir(),
		Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64, Nullable: false},
			{Name: "age", Type: TypeInt64, Nullable: true},
		}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s.Close()
	ctx := context.Background()
	// seg1：age 全 NULL
	for i := 1; i <= 3; i++ {
		if err := s.Append(ctx, Row{"id": int64(i), "age": nil}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	// seg2：age 有值
	for i := 4; i <= 6; i++ {
		if err := s.Append(ctx, Row{"id": int64(i), "age": int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	rows, considered, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreater, Value: int64(0)}},
	})
	logCase(t, biz, "全NULL段被跳过, 有值段保留", "ids 4..6", map[string]any{"ids": idsOf(rows), "considered": considered, "skipped": skipped})
	if skipped != 1 || !int64SliceEq(idsOf(rows), []int64{4, 5, 6}) {
		t.Fatalf("all-null segment must be skipped: ids=%v skipped=%d", idsOf(rows), skipped)
	}

	// 全列读不受跳过影响，6 行都在
	all, _, _ := scanWithPruning(t, s, ScanOptions{})
	logCase(t, biz, "无过滤全NULL行仍在", "6 rows", len(all))
	if len(all) != 6 {
		t.Fatalf("full scan must include null rows, got %d", len(all))
	}
}

// TestSegmentPruningStatMissing 旧段（清单里没有 stats 依据）必须安全回退
// 为完整扫描：该跳的也不跳，结果完整。
func TestSegmentPruningStatMissing(t *testing.T) {
	biz := "段级跳过-依据缺失回退全扫"
	s := buildZoneStore(t)
	defer s.Close()

	// 手工抹掉"最后一个段"的依据（age>=40 唯一会命中的段）。该段依据缺失
	// 后不能被跳过，必须完整读出；其余 3 段依据完好仍被跳过。
	s.mu.Lock()
	orig := s.m.Segments[3].Stats
	s.m.Segments[3].Stats = nil
	s.mu.Unlock()

	rows, considered, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(40)}},
	})
	logCase(t, biz, "命中段无依据：宁可不跳也不漏", "skipped=3, 结果仍完整", map[string]any{"rows": len(rows), "considered": considered, "skipped": skipped, "ids": idsOf(rows)})
	if skipped != 3 {
		t.Fatalf("segments with valid stats must still be skipped, got skipped=%d", skipped)
	}
	if !int64SliceEq(idsOf(rows), []int64{16, 17, 18, 19, 20}) {
		t.Fatalf("result must be complete with missing stat, got %v", idsOf(rows))
	}

	// 抹掉全部依据：一个段都不能跳，结果仍完整。
	s.mu.Lock()
	for i := range s.m.Segments {
		s.m.Segments[i].Stats = nil
	}
	s.mu.Unlock()
	rows, _, skipped = scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(40)}},
	})
	logCase(t, biz, "全部段无依据：完整扫描", "skipped=0, rows=5", map[string]any{"skipped": skipped, "rows": len(rows)})
	if skipped != 0 || len(rows) != 5 {
		t.Fatalf("with all stats missing nothing may be skipped: rows=%d skipped=%d", len(rows), skipped)
	}
	_ = orig
}

// TestSegmentPruningStatCorruptOnDisk 磁盘上清单里的依据 CRC 损坏：依据在
// 跳过决策时即被判为不可信，安全回退完整扫描，结果一行不少。
func TestSegmentPruningStatCorruptOnDisk(t *testing.T) {
	biz := "段级跳过-依据损坏回退全扫"
	dir := t.TempDir()
	s := mustCreate(t, dir, zoneSchema(), Options{FlushThreshold: 100, CompactionMinSegments: 100})
	ctx := context.Background()
	for g := 0; g < 2; g++ {
		for k := 0; k < 3; k++ {
			age := int64(g*10 + 10 + k)
			if err := s.Append(ctx, Row{"id": int64(g*3 + k + 1), "age": age}); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Flush(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 改坏清单里 seg1 age 依据的 max（但不改 CRC），制造 CRC 不自洽。
	mPath := manifestPath(dir)
	raw, err := os.ReadFile(mPath)
	if err != nil {
		t.Fatal(err)
	}
	var m manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	st := m.Segments[0].Stats["age"]
	st.IntMax = 999 // 改极值，CRC 仍旧 -> CRC 校验失败
	m.Segments[0].Stats["age"] = st
	corrupt, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(mPath, corrupt, 0o644); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s2.Close()
	// seg1 age[10,12] 依据 CRC 损坏 -> 不可信，完整读出（其行 age<15 被过滤
	// 正常排除）；seg2 age[20,22] 依据完好但与 age>=15 重叠，本就不跳过。
	rows, considered, skipped := scanWithPruning(t, s2, ScanOptions{
		Filters: []Filter{{Column: "age", Op: OpGreaterEqual, Value: int64(15)}},
	})
	// 正确结果是 seg2 的 3 行；关键断言：损坏的 seg1 没有被错误跳过，因此
	// 它若含命中行也一定能被读出（本例不含，但 skipped 必须为 0）。
	logCase(t, biz, "损坏依据不可信，完整扫描", "3 rows, skipped=0", map[string]any{"rows": len(rows), "considered": considered, "skipped": skipped})
	if !int64SliceEq(idsOf(rows), []int64{4, 5, 6}) {
		t.Fatalf("corrupt stats full-scan result wrong: %v", idsOf(rows))
	}
	if skipped != 0 {
		t.Fatalf("corrupt stats segment must not be skipped, got skipped=%d", skipped)
	}
}

// TestSegmentPruningStatLyingDetected 依据 CRC 自洽但与真实数据对不上
// （模拟"被人连 CRC 一起伪造"或更严重的逻辑错误）：
//   - seg1 被伪造成不可能命中 age>=35（真实含 age 40..44 的命中行），若信
//     了依据就会漏掉 5 行；
//   - seg2 同样伪造但会被实际读到，交叉校验立刻识破；
//   - 读取放弃首轮结果、完整重扫，最终结果与不跳过时逐行一致，不漏不多。
func TestSegmentPruningStatLyingDetected(t *testing.T) {
	biz := "段级跳过-依据与数据对不上时重扫"
	s := buildZoneStore(t)
	defer s.Close()

	s.mu.Lock()
	// seg1 真实 [10,14]：伪造成 [100,104]，让 age>=35 错误地跳过它（它本就
	// 不含命中行，不会造成漏行，但证明它被错误跳过了）。
	lie1 := s.m.Segments[0].Stats["age"]
	lie1.IntV, lie1.IntMax = 100, 104
	lie1.CRC = statCRC(lie1)
	s.m.Segments[0].Stats["age"] = lie1
	// seg4 真实 [40,44] 含 5 个命中行：伪造成 [0,4]，使 age>=35 错误跳过它
	// ——若没有交叉校验与重扫，这 5 行必丢。
	lie4 := s.m.Segments[3].Stats["age"]
	lie4.IntV, lie4.IntMax = 0, 4
	lie4.CRC = statCRC(lie4)
	s.m.Segments[3].Stats["age"] = lie4
	// seg2 真实 [20,24]：伪造成与真实不符的区间，但它在 age>=35 下依据看似
	// 不命中同样会被跳过……为保证有段被实际读到而识破，让 seg3 依据保持
	// 真实（[30,34] 不含 >=35，会被正常跳过）。因此这里改为让 seg2 的伪造
	// 区间与 age>=35 重叠（伪造成 [20,999]），它会被读到并立刻露馅。
	lie2 := s.m.Segments[1].Stats["age"]
	lie2.IntV, lie2.IntMax = 20, 999
	lie2.CRC = statCRC(lie2)
	s.m.Segments[1].Stats["age"] = lie2
	s.mu.Unlock()

	f := Filter{Column: "age", Op: OpGreaterEqual, Value: int64(35)}
	full := fullScanWithoutPruning(t, s, f)

	rows, considered, skipped := scanWithPruning(t, s, ScanOptions{Filters: []Filter{f}})
	logCase(t, biz, "三处伪造：首轮会误跳seg4(含命中行)，读seg2识破后完整重扫",
		map[string]any{"correctRows": len(full), "ids": idsOf(full)},
		map[string]any{"rows": len(rows), "considered": considered, "skippedAfterFallback": skipped})
	if skipped != 0 {
		t.Fatalf("after fallback no segment may remain skipped, got %d", skipped)
	}
	if !int64SliceEq(idsOf(rows), idsOf(full)) {
		t.Fatalf("lying stats must still yield the complete result: got=%v want=%v",
			idsOf(rows), idsOf(full))
	}
	// 明确断言被错误排除的 seg4 五行被捞了回来。
	if !int64SliceEq(idsOf(rows), []int64{16, 17, 18, 19, 20}) {
		t.Fatalf("seg4 rows (16..20) wrongly skipped must be recovered, got %v", idsOf(rows))
	}
}

// TestPruningAcrossRename 改名后段级跳过依据经身份映射仍然有效；
// 同名重生后绝不拿旧列依据错误排除新列（新列无依据 -> 不跳过）。
func TestPruningAcrossRename(t *testing.T) {
	biz := "段级跳过-改名与重生"
	s := mustCreate(t, t.TempDir(), Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "age", Type: TypeInt64, Nullable: true},
	}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})
	defer s.Close()
	ctx := context.Background()
	for i := int64(1); i <= 3; i++ {
		if err := s.Append(ctx, Row{"id": i, "age": i * 10}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.RenameColumn(ctx, "age", "years"); err != nil {
		t.Fatal(err)
	}
	for i := int64(4); i <= 6; i++ {
		if err := s.Append(ctx, Row{"id": i, "years": i * 10}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 用新名字 years>=40：seg1 依据记录在旧名 age 上，经身份映射后同样被跳过
	rows, considered, skipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "years", Op: OpGreaterEqual, Value: int64(40)}},
	})
	logCase(t, biz, "改名后用新名字过滤，旧段依据仍生效", "ids 4..6, skip seg1",
		map[string]any{"ids": idsOf(rows), "considered": considered, "skipped": skipped})
	if !int64SliceEq(idsOf(rows), []int64{4, 5, 6}) || skipped != 1 {
		t.Fatalf("pruning across rename broken: ids=%v skipped=%d", idsOf(rows), skipped)
	}

	// 同名重生：years 下线后以 string 重新上线，旧依据是旧身份，对新列无效
	if err := s.DropColumn(ctx, "years"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddColumn(ctx, Column{Name: "years", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Row{"id": int64(7), "years": "senior"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	rrows, _, rskipped := scanWithPruning(t, s, ScanOptions{
		Filters: []Filter{{Column: "years", Op: OpEqual, Value: "senior"}},
	})
	logCase(t, biz, "重生列：旧int依据不得用于string新列", "id=7, 旧段不依据跳过",
		map[string]any{"ids": idsOf(rrows), "skipped": rskipped})
	if !int64SliceEq(idsOf(rrows), []int64{7}) {
		t.Fatalf("reborn column must not reuse old stats: %v", idsOf(rrows))
	}
	if rskipped != 0 {
		t.Fatalf("reborn column segments must not be pruned by stale identity stats, skipped=%d", rskipped)
	}
}
