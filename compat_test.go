package columnar

// 升级兼容性测试：输入是"升级前那一版"（fc50cac，段格式 v1、清单无列
// 生命周期 ID）真实写出的存储目录 testdata/legacy-v1（由
// scripts/gen_legacy_store.sh 生成，勿手工编辑）。
//
// 老库内容（55 行、6 段、5 个结构版本）：
//   - i=0..24   : id=int64(i), name, score, active；无 tag 列（应读 NULL）
//   - i=25..34  : 同上 + tag="tag-i%5"（AddColumn 之后）
//   - i=35..44  : id 以 float64 写入（AlterColumnType int64->float64 之后）
//   - i=45..54  : DropColumn(tag) 又同名加回之后写入
// 当前结构：id float64, name string, score float64, active bool, tag string。

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const legacyStoreDir = "testdata/legacy-v1"

// copyLegacyStore 把老库复制到临时目录（测试要往里写，不能动 testdata）。
func copyLegacyStore(t *testing.T) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "legacy")
	entries, err := os.ReadDir(legacyStoreDir)
	if err != nil {
		t.Fatalf("read %s: %v（老库输入缺失，可用 bash scripts/gen_legacy_store.sh 重新生成）", legacyStoreDir, err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(legacyStoreDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}

// legacyExpectedRow 按生成规则重算第 i 行的期望值（当前结构下）。
func legacyExpectedRow(i int) Row {
	r := Row{
		"id":     float64(i), // int64 老字节经 int64->float64 演进读出
		"name":   fmt.Sprintf("name-%03d", i),
		"score":  float64(i) * 1.5,
		"active": i%2 == 0,
		"tag":    nil, // AddColumn 之前写入的行
	}
	if i >= 25 {
		r["tag"] = fmt.Sprintf("tag-%d", i%5)
	}
	return r
}

func rowsEqual(t *testing.T, got, want []Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("row count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		for k, wv := range want[i] {
			if got[i][k] != wv {
				t.Fatalf("row %d column %q = %#v, want %#v", i, k, got[i][k], wv)
			}
		}
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d has %d columns, want %d", i, len(got[i]), len(want[i]))
		}
	}
}

func legacyExpectedAll() []Row {
	rows := make([]Row, 55)
	for i := range rows {
		rows[i] = legacyExpectedRow(i)
	}
	return rows
}

// TestCompatOpenLegacyStore 老库打开 + 全列读：55 行全部原样读出。
func TestCompatOpenLegacyStore(t *testing.T) {
	dir := copyLegacyStore(t)
	t.Logf("输入: %s（升级前版本写出的老库，55 行 6 段）", legacyStoreDir)
	s := mustOpen(t, dir, DefaultOptions())
	defer s.Close()
	got := scanAll(t, s, ScanOptions{})
	rowsEqual(t, got, legacyExpectedAll())
	t.Logf("全列读: %d 行，与升级前写入完全一致", len(got))
}

// TestCompatLegacyProjection 老库列裁剪读：只取子集列，值与全列读一致。
func TestCompatLegacyProjection(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	defer s.Close()

	got := scanAll(t, s, ScanOptions{Columns: []string{"tag", "id"}})
	if len(got) != 55 {
		t.Fatalf("projection rows = %d, want 55", len(got))
	}
	for i, r := range got {
		if len(r) != 2 {
			t.Fatalf("row %d projected to %d columns, want 2", i, len(r))
		}
		want := legacyExpectedRow(i)
		if r["id"] != want["id"] || r["tag"] != want["tag"] {
			t.Fatalf("row %d = %v, want id=%v tag=%v", i, r, want["id"], want["tag"])
		}
	}
	t.Logf("列裁剪读: 55 行 x {id,tag}，含 AddColumn 前的 NULL 行，全部一致")
}

// TestCompatLegacyFilter 老库过滤读：v1 段没有统计块，过滤自动退回完整
// 扫描，结果必须与真实数据一致（不少行、不多行）。
func TestCompatLegacyFilter(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	defer s.Close()

	cases := []struct {
		name string
		f    Filter
		want func(i int) bool
	}{
		{"id>50(float64列)", Filter{Column: "id", Op: OpGreater, Value: float64(50)}, func(i int) bool { return i > 50 }},
		{"active==true", Filter{Column: "active", Op: OpEqual, Value: true}, func(i int) bool { return i%2 == 0 }},
		{"tag==tag-1(含NULL行)", Filter{Column: "tag", Op: OpEqual, Value: "tag-1"}, func(i int) bool { return i >= 25 && i%5 == 1 }},
		{"score<7.5", Filter{Column: "score", Op: OpLess, Value: 7.5}, func(i int) bool { return i < 5 }},
	}
	for _, tc := range cases {
		got := scanAll(t, s, ScanOptions{Filters: []Filter{tc.f}})
		var wantN int
		for i := 0; i < 55; i++ {
			if tc.want(i) {
				wantN++
			}
		}
		if len(got) != wantN {
			t.Fatalf("filter %s: got %d rows, want %d", tc.name, len(got), wantN)
		}
		for _, r := range got {
			i := int(r["id"].(float64))
			if !tc.want(i) {
				t.Fatalf("filter %s: unexpected row id=%d", tc.name, i)
			}
		}
		t.Logf("过滤读 %-22s -> %d 行，正确", tc.name, len(got))
	}
}

// TestCompatLegacyReopen 关闭重开：两次打开读到的老数据完全一致。
func TestCompatLegacyReopen(t *testing.T) {
	dir := copyLegacyStore(t)
	s1 := mustOpen(t, dir, DefaultOptions())
	first := scanAll(t, s1, ScanOptions{})
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	s2 := mustOpen(t, dir, DefaultOptions())
	defer s2.Close()
	second := scanAll(t, s2, ScanOptions{})
	rowsEqual(t, second, first)
	rowsEqual(t, second, legacyExpectedAll())
	t.Logf("关闭重开: 两次全列读均为 %d 行且完全一致", len(second))
}

// TestCompatLegacyManifestNormalized 打开老库时清单被一次性归一化：
// 补上列生命周期 ID 与 next_column_id 并原子落盘；归一化幂等，再开不变。
func TestCompatLegacyManifestNormalized(t *testing.T) {
	dir := copyLegacyStore(t)
	rawBefore := readFileBytes(t, filepath.Join(dir, manifestName))
	if strings.Contains(string(rawBefore), "next_column_id") {
		t.Fatalf("输入老库清单不应含 next_column_id（应来自升级前版本）:\n%s", rawBefore)
	}

	s := mustOpen(t, dir, DefaultOptions())
	sch := s.Schema()
	for _, c := range sch.Columns {
		if c.ID == 0 {
			t.Fatalf("归一化后列 %q 仍无生命周期 ID", c.Name)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rawAfter := readFileBytes(t, filepath.Join(dir, manifestName))
	if !strings.Contains(string(rawAfter), "next_column_id") {
		t.Fatalf("归一化后清单应含 next_column_id:\n%s", rawAfter)
	}
	t.Logf("清单归一化: 当前结构 %d 列全部获得生命周期 ID，next_column_id 已落盘", len(sch.Columns))

	// 幂等：再次打开，清单逐字节不变，数据不变。
	s2 := mustOpen(t, dir, DefaultOptions())
	defer s2.Close()
	if raw2 := readFileBytes(t, filepath.Join(dir, manifestName)); string(raw2) != string(rawAfter) {
		t.Fatalf("归一化不是幂等的：第二次打开又改写了清单")
	}
	rowsEqual(t, scanAll(t, s2, ScanOptions{}), legacyExpectedAll())
	t.Logf("幂等性: 再次打开清单逐字节不变，55 行数据一致")
}

// TestCompatLegacyAppend 老库继续在线追加：新行落 v2 段，与老 v1 段混读，
// 重开后新老数据都完整、不重不错。
func TestCompatLegacyAppend(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	ctx := t.Context()
	for i := 55; i < 70; i++ {
		if err := s.Append(ctx, legacyExpectedRow(i)); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir, DefaultOptions())
	defer s2.Close()
	want := legacyExpectedAll()
	for i := 55; i < 70; i++ {
		want = append(want, legacyExpectedRow(i))
	}
	rowsEqual(t, scanAll(t, s2, ScanOptions{}), want)
	t.Logf("继续追加: 老 55 行 + 新 15 行混读一致，重开后依然一致")
}

// TestCompatLegacyCompact 老库触发合并：v1 老段被重写成 v2 新段，
// 老值保持不变，重开后一致。
func TestCompatLegacyCompact(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	before := scanAll(t, s, ScanOptions{})
	if err := s.Compact(t.Context()); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if n := s.SegmentCount(); n != 1 {
		t.Fatalf("合并后段数 = %d, want 1", n)
	}
	rowsEqual(t, scanAll(t, s, ScanOptions{}), before)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 合并产物是 v2 段（带统计块），重开后读法与值都不变。
	seg := filepath.Join(dir, segmentFileName(7)) // NextSegmentID 之后的新段
	head := readFileBytes(t, seg)
	if got := int(head[4])<<8 | int(head[5]); got != int(segmentFormatVersion) {
		t.Fatalf("合并产出段格式版本 = %d, want %d", got, segmentFormatVersion)
	}
	s2 := mustOpen(t, dir, DefaultOptions())
	defer s2.Close()
	rowsEqual(t, scanAll(t, s2, ScanOptions{}), legacyExpectedAll())
	t.Logf("小文件合并: 6 个 v1 老段重写为 1 个 v2 段，55 行老值不变，重开一致")
}

// TestCompatLegacySchemaEvolution 老库升级后做结构演进：
//   - 改名 name->title：老数据（含 v1 老段）在新名字下连续可读；
//   - 删 tag 再同名加回：升级后发生的同名重生，老字节读 NULL（新语义），
//     新写入的行有值；
//   - 新增列：老行读 NULL。
func TestCompatLegacySchemaEvolution(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	ctx := t.Context()

	if err := s.RenameColumn(ctx, "name", "title"); err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}
	got := scanAll(t, s, ScanOptions{Columns: []string{"id", "title"}})
	if len(got) != 55 {
		t.Fatalf("改名后行数 = %d, want 55", len(got))
	}
	for i, r := range got {
		want := fmt.Sprintf("name-%03d", i)
		if r["title"] != want {
			t.Fatalf("row %d title = %v, want %s（v1 老段数据必须在改名后连续可读）", i, r["title"], want)
		}
	}
	t.Logf("改名: name->title，55 行老数据（含 v1 段）在新名字下连续可读")

	if err := s.DropColumn(ctx, "tag"); err != nil {
		t.Fatalf("DropColumn: %v", err)
	}
	if err := s.AddColumn(ctx, Column{Name: "tag", Type: TypeString, Nullable: true}); err != nil {
		t.Fatalf("AddColumn(tag): %v", err)
	}
	if err := s.AddColumn(ctx, Column{Name: "extra", Type: TypeInt64, Nullable: true}); err != nil {
		t.Fatalf("AddColumn(extra): %v", err)
	}
	newRow := Row{"id": float64(1000), "title": "new", "score": 1.5, "active": true, "tag": "fresh", "extra": int64(7)}
	if err := s.Append(ctx, newRow); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir, DefaultOptions())
	defer s2.Close()
	all := scanAll(t, s2, ScanOptions{})
	if len(all) != 56 {
		t.Fatalf("演进+追加后行数 = %d, want 56", len(all))
	}
	for i := 0; i < 55; i++ {
		r := all[i]
		if r["tag"] != nil {
			t.Fatalf("row %d: 升级后同名重生的 tag 应读 NULL（老字节不可错位复用），got %v", i, r["tag"])
		}
		if r["extra"] != nil {
			t.Fatalf("row %d: 新增列 extra 应读 NULL, got %v", i, r["extra"])
		}
		if r["title"] != fmt.Sprintf("name-%03d", i) {
			t.Fatalf("row %d: title = %v", i, r["title"])
		}
	}
	last := all[55]
	if last["tag"] != "fresh" || last["extra"] != int64(7) || last["title"] != "new" {
		t.Fatalf("新写入行 = %v", last)
	}
	t.Logf("结构演进: 删列同名重生后老行 tag=NULL、新行有值；新增列老行 NULL；重开一致")
}

// TestCompatUnsupportedVersion 真正无法兼容的段格式版本（来自更新的实现）：
// 打开存储时即报 ErrUnsupportedVersion，与 ErrCorruptData 严格可分，
// 不会拖到读取、更不会只读一半。
func TestCompatUnsupportedVersion(t *testing.T) {
	for _, v := range []uint16{0, 3, 99} {
		t.Run(fmt.Sprintf("v%d", v), func(t *testing.T) {
			dir := copyLegacyStore(t)
			p := filepath.Join(dir, "seg-0000000001.ccs")
			data := readFileBytes(t, p)
			data[4], data[5] = byte(v>>8), byte(v)
			if err := os.WriteFile(p, data, 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := Open(dir, DefaultOptions())
			if err == nil {
				t.Fatalf("版本 %d: Open 应当即失败", v)
			}
			if !errors.Is(err, ErrUnsupportedVersion) {
				t.Fatalf("版本 %d: 错误应可断言为 ErrUnsupportedVersion, got %v", v, err)
			}
			if errors.Is(err, ErrCorruptData) {
				t.Fatalf("版本 %d: 不兼容不得伪装成数据损坏: %v", v, err)
			}
			t.Logf("版本 %d: 打开时即报 %v（与损坏可分）", v, err)
		})
	}
}

// TestCompatCorruptStillReported 老库文件真被改坏：仍照常报 ErrCorruptData，
// 且不会被误判成版本不兼容。
func TestCompatCorruptStillReported(t *testing.T) {
	t.Run("坏魔数打开即报", func(t *testing.T) {
		dir := copyLegacyStore(t)
		p := filepath.Join(dir, "seg-0000000002.ccs")
		data := readFileBytes(t, p)
		data[0] ^= 0xFF
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := Open(dir, DefaultOptions())
		if !errors.Is(err, ErrCorruptData) || errors.Is(err, ErrUnsupportedVersion) {
			t.Fatalf("坏魔数应报 ErrCorruptData（且不是版本问题）, got %v", err)
		}
		t.Logf("坏魔数: 打开时报 %v", err)
	})
	t.Run("数据帧字节损坏读取时报", func(t *testing.T) {
		dir := copyLegacyStore(t)
		p := filepath.Join(dir, "seg-0000000002.ccs")
		data := readFileBytes(t, p)
		data[len(data)/2] ^= 0xFF // 翻数据区一个字节，头部保持完好
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
		s := mustOpen(t, dir, DefaultOptions())
		defer s.Close()
		sc, err := s.Scan(t.Context(), ScanOptions{})
		if err != nil {
			t.Fatalf("Scan: %v", err)
		}
		defer sc.Close()
		var scanErr error
		for {
			if _, err := sc.Next(); err != nil {
				scanErr = err
				break
			}
		}
		if !errors.Is(scanErr, ErrCorruptData) || errors.Is(scanErr, ErrUnsupportedVersion) {
			t.Fatalf("帧损坏应报 ErrCorruptData（且不是版本问题）, got %v", scanErr)
		}
		t.Logf("帧字节损坏: 读取时报 %v", scanErr)
	})
}

// TestCompatLegacyMixedSegmentsFilter 老库追加新数据后形成 v1/v2 混合段：
// 过滤读时 v2 段走统计块跳过、v1 段自动退回完整扫描，混合结果必须正确。
func TestCompatLegacyMixedSegmentsFilter(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	ctx := t.Context()
	for i := 55; i < 70; i++ { // 新行落 v2 段（带统计块）
		if err := s.Append(ctx, legacyExpectedRow(i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, s, ScanOptions{Filters: []Filter{
		{Column: "id", Op: OpGreaterEqual, Value: float64(50)},
	}})
	if len(got) != 20 { // i=50..69
		t.Fatalf("混合段过滤行数 = %d, want 20", len(got))
	}
	for _, r := range got {
		if i := int(r["id"].(float64)); i < 50 || i > 69 {
			t.Fatalf("混合段过滤出现越界行 id=%d", i)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("混合段过滤: v1 老段(全扫)+v2 新段(统计跳过) 结果 20 行，正确")
}

// TestCompatLegacyCompactCrashWindow 老库在"新段落盘后、清单交换前"的合并
// 崩溃窗口：重启回到合并前一致状态，55 行老数据不丢不重。
func TestCompatLegacyCompactCrashWindow(t *testing.T) {
	dir := copyLegacyStore(t)
	s := mustOpen(t, dir, DefaultOptions())
	s.SetFailPoint("compactFailAfterCommit", true)
	if err := s.Compact(t.Context()); err == nil {
		t.Fatal("故障注入下 Compact 应返回错误")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := mustOpen(t, dir, DefaultOptions()) // 重开时清理无主新段
	defer s2.Close()
	rowsEqual(t, scanAll(t, s2, ScanOptions{}), legacyExpectedAll())
	if n := s2.SegmentCount(); n != 6 {
		t.Fatalf("崩溃恢复后段数 = %d, want 6（回到合并前）", n)
	}
	t.Logf("合并崩溃窗口: 重开后回到合并前 6 段 55 行，无主新段已清理")
}
