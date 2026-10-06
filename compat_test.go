package columnar

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// 本文件的测试以"升级前那一版写出来的存储"为输入：
// 段文件是 v1 格式（固定头 version=1、无统计块），清单没有
// next_column_id 字段、列没有生命周期 ID。这里用与旧版完全相同的
// 布局现场构造老库，再验证新版本打开后的所有读写路径。

// ---------- 老版本格式 fixture ----------

// legacyColumnJSON / legacySchemaJSON / legacyManifestJSON 精确镜像
// 升级前版本的清单 JSON 形状（列无 ID 字段、清单无 next_column_id）。
type legacyColumnJSON struct {
	Name     string
	Nullable bool
	Type     DataType
}

type legacySchemaJSON struct {
	Version int
	Columns []legacyColumnJSON
}

type legacyManifestJSON struct {
	Version       int                `json:"version"`
	SchemaHistory []legacySchemaJSON `json:"schema_history"`
	Segments      []segmentMeta      `json:"segments"`
	NextSegmentID int64              `json:"next_segment_id"`
}

// legacyEncodeSegmentV1 按升级前版本的布局编码段文件：
// 固定头(version=1) + 列目录 + 列帧，没有统计块。
func legacyEncodeSegmentV1(schemaVersion int, rows []Row, schema Schema) []byte {
	var frames [][]byte
	var buf bytes.Buffer
	buf.WriteString(segmentMagic)
	_ = binary.Write(&buf, binary.BigEndian, segmentFormatVersionV1)
	_ = binary.Write(&buf, binary.BigEndian, uint32(schemaVersion))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(rows)))
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(schema.Columns)))
	for _, col := range schema.Columns {
		frame, err := buildColumnFrame(col, rows)
		if err != nil {
			panic(err)
		}
		nb := []byte(col.Name)
		buf.WriteByte(byte(len(nb)))
		buf.Write(nb)
		buf.WriteByte(byte(col.Type))
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(frame)))
		buf.Write(lb[:])
		frames = append(frames, frame)
	}
	for _, f := range frames {
		buf.Write(f)
	}
	return buf.Bytes()
}

// legacyFixture 描述一个脚本化的"老版本会话"产物。
type legacyFixture struct {
	// 老库当前（最后）结构：id, score, name, tag, active（active 是删列后
	// 同名再加的列；老版本按名匹配，老段里的 active 值升级前仍然可见）。
	expectedAll []Row // 升级前全列读的结果（升级后必须完全一致）
}

// buildLegacyStore 用老版本格式现场构造一个存储目录，模拟的老会话：
//
//	v1: id int64 NN, score float64, name string, active bool   -> 写 seg1（2 行）
//	v2: + tag string nullable                                  -> 写 seg2（2 行）
//	v3: - active
//	v4: + active bool（同名重生；老版本语义下老值按名可见）
//
// 返回升级前全列读应得的全部行（按写入顺序）。
func buildLegacyStore(t *testing.T, dir string) []Row {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	mkCols := func(sch Schema) []legacyColumnJSON {
		out := make([]legacyColumnJSON, len(sch.Columns))
		for i, c := range sch.Columns {
			out[i] = legacyColumnJSON{Name: c.Name, Nullable: c.Nullable, Type: c.Type}
		}
		return out
	}

	v1 := Schema{Version: 1, Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "score", Type: TypeFloat64, Nullable: true},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "active", Type: TypeBool, Nullable: true},
	}}
	seg1Rows := []Row{
		{"id": int64(1), "score": 1.5, "name": "alice", "active": true},
		{"id": int64(2), "score": nil, "name": nil, "active": false},
	}
	v2 := Schema{Version: 2, Columns: append(append([]Column{}, v1.Columns...),
		Column{Name: "tag", Type: TypeString, Nullable: true})}
	seg2Rows := []Row{
		{"id": int64(3), "score": 3.5, "name": "carol", "active": true, "tag": "x"},
		{"id": int64(4), "score": 4.5, "name": "dave", "active": false, "tag": nil},
	}
	v3 := Schema{Version: 3, Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "score", Type: TypeFloat64, Nullable: true},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "tag", Type: TypeString, Nullable: true},
	}}
	v4 := Schema{Version: 4, Columns: append(append([]Column{}, v3.Columns...),
		Column{Name: "active", Type: TypeBool, Nullable: true})}

	segs := []segmentMeta{
		{ID: 1, FileName: segmentFileName(1), RowCount: len(seg1Rows), SchemaVersion: 1},
		{ID: 2, FileName: segmentFileName(2), RowCount: len(seg2Rows), SchemaVersion: 2},
	}
	if err := os.WriteFile(filepath.Join(dir, segs[0].FileName),
		legacyEncodeSegmentV1(1, seg1Rows, v1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, segs[1].FileName),
		legacyEncodeSegmentV1(2, seg2Rows, v2), 0o644); err != nil {
		t.Fatal(err)
	}
	lm := legacyManifestJSON{
		Version: 6,
		SchemaHistory: []legacySchemaJSON{
			{Version: 1, Columns: mkCols(v1)},
			{Version: 2, Columns: mkCols(v2)},
			{Version: 3, Columns: mkCols(v3)},
			{Version: 4, Columns: mkCols(v4)},
		},
		Segments:      segs,
		NextSegmentID: 3,
	}
	data, err := json.MarshalIndent(lm, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath(dir), data, 0o644); err != nil {
		t.Fatal(err)
	}

	// 升级前全列读（按名匹配）的结果：列序按当前结构 v4。
	all := make([]Row, 0, 4)
	for _, r := range append(append([]Row{}, seg1Rows...), seg2Rows...) {
		all = append(all, Row{
			"id": r["id"], "score": r["score"], "name": r["name"],
			"tag": r["tag"], "active": r["active"],
		})
	}
	return all
}

// scanAllErr 与 scanAll 类似，但把错误返回给调用方（用于断言错误类别）。
func scanAllErr(s *Store, opts ScanOptions) ([]Row, error) {
	sc, err := s.Scan(context.Background(), opts)
	if err != nil {
		return nil, err
	}
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

func projectRows(rows []Row, cols ...string) []Row {
	out := make([]Row, len(rows))
	for i, r := range rows {
		pr := Row{}
		for _, c := range cols {
			pr[c] = r[c]
		}
		out[i] = pr
	}
	return out
}

// ---------- 读取路径 ----------

func TestUpgradeLegacyStoreReadPaths(t *testing.T) {
	biz := "版本兼容-老库全列/裁剪/过滤/重开读取"
	dir := t.TempDir()
	wantAll := buildLegacyStore(t, dir)
	logCase(t, biz, "输入", "老版本写出的目录(2个v1段,4个结构版本,含删列重加)", fmt.Sprintf("%d 行", len(wantAll)))

	s := mustOpen(t, dir, DefaultOptions())

	// 1. 全列读
	got := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "全列读", "scan *", got)
	if !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("全列读不一致:\n got=%v\nwant=%v", got, wantAll)
	}

	// 2. 列裁剪读
	wantProj := projectRows(wantAll, "id", "name")
	gotProj := scanAll(t, s, ScanOptions{Columns: []string{"id", "name"}})
	logCase(t, biz, "列裁剪读", "project id,name", gotProj)
	if !reflect.DeepEqual(gotProj, wantProj) {
		t.Fatalf("列裁剪读不一致:\n got=%v\nwant=%v", gotProj, wantProj)
	}

	// 3. 过滤读（v1 段无统计块，必须回退完整扫描且结果正确）
	filters := []Filter{{Column: "id", Op: OpGreaterEqual, Value: int64(3)}}
	gotFiltered := scanAll(t, s, ScanOptions{Filters: filters})
	wantFiltered := wantAll[2:]
	logCase(t, biz, "过滤读", "id>=3", gotFiltered)
	if !reflect.DeepEqual(gotFiltered, wantFiltered) {
		t.Fatalf("过滤读不一致:\n got=%v\nwant=%v", gotFiltered, wantFiltered)
	}
	// 过滤在"段写入后才新增的列"上：老段该列全 NULL，只能命中 seg2 的行。
	gotTag := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "tag", Op: OpEqual, Value: "x"}}})
	logCase(t, biz, "过滤读-后增列", `tag=="x"`, gotTag)
	if !reflect.DeepEqual(gotTag, wantAll[2:3]) {
		t.Fatalf("后增列过滤读不一致:\n got=%v\nwant=%v", gotTag, wantAll[2:3])
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 4. 关闭重开：所有读取方式结果完全一致
	s2 := mustOpen(t, dir, DefaultOptions())
	got2 := scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "关闭重开后全列读", "reopen + scan *", got2)
	if !reflect.DeepEqual(got2, wantAll) {
		t.Fatalf("重开后全列读不一致:\n got=%v\nwant=%v", got2, wantAll)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 归一化 ----------

func TestUpgradeLegacyManifestNormalized(t *testing.T) {
	biz := "版本兼容-老清单归一化"
	dir := t.TempDir()
	buildLegacyStore(t, dir)

	s := mustOpen(t, dir, DefaultOptions())
	m1, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "首次打开后清单", "next_column_id/列ID", fmt.Sprintf("next=%d", m1.NextColumnID))
	if m1.NextColumnID <= 0 {
		t.Fatalf("归一化后 NextColumnID 必须为正, got %d", m1.NextColumnID)
	}
	for _, sch := range m1.SchemaHistory {
		for _, c := range sch.Columns {
			if c.ID == 0 {
				t.Fatalf("归一化后 schema v%d 列 %q 仍无生命周期 ID", sch.Version, c.Name)
			}
		}
	}
	// 同名列（含老版本删了再加的 active）共享同一 ID：保持升级前按名匹配的读结果。
	var activeIDs []int
	for _, sch := range m1.SchemaHistory {
		if c, ok := sch.Column("active"); ok {
			activeIDs = append(activeIDs, c.ID)
		}
	}
	logCase(t, biz, "同名重生列回填", "active 在各版本的 ID", activeIDs)
	for _, id := range activeIDs[1:] {
		if id != activeIDs[0] {
			t.Fatalf("老清单同名列应回填同一 ID 以保持升级前读结果, got %v", activeIDs)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 幂等：再次打开不应再改写清单。
	s2 := mustOpen(t, dir, DefaultOptions())
	m2, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "二次打开", "清单版本", fmt.Sprintf("v%d -> v%d", m1.Version, m2.Version))
	if m2.Version != m1.Version {
		t.Fatalf("归一化应幂等, 二次打开不应再改清单: v%d -> v%d", m1.Version, m2.Version)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 升级后继续在线使用 ----------

func TestUpgradeLegacyStoreContinueWriteAndCompact(t *testing.T) {
	biz := "版本兼容-老库继续写入与合并"
	dir := t.TempDir()
	wantAll := buildLegacyStore(t, dir)
	opt := Options{FlushThreshold: 100, CompactionMinSegments: 3}

	s := mustOpen(t, dir, opt)
	// 继续追加：新数据走 v2 段，与老 v1 段混读。
	newRow := Row{"id": int64(5), "score": 5.5, "name": "eve", "tag": "y", "active": true}
	if err := s.Append(context.Background(), newRow); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantAll = append(wantAll, newRow)
	got := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "追加后全列读", "老v1段+新v2段混读", got)
	if !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("追加后读取不一致:\n got=%v\nwant=%v", got, wantAll)
	}

	// 触发合并：老 v1 段被重写成 v2 段，老值必须保持不变。
	if err := s.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	logCase(t, biz, "合并后段数", "Compact", s.SegmentCount())
	if s.SegmentCount() != 1 {
		t.Fatalf("合并后应为 1 段, got %d", s.SegmentCount())
	}
	got = scanAll(t, s, ScanOptions{})
	logCase(t, biz, "合并后全列读", "重写后的v2段", got)
	if !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("合并后老值被改变:\n got=%v\nwant=%v", got, wantAll)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重开仍一致。
	s2 := mustOpen(t, dir, opt)
	got = scanAll(t, s2, ScanOptions{})
	logCase(t, biz, "合并重开后全列读", "reopen + scan *", got)
	if !reflect.DeepEqual(got, wantAll) {
		t.Fatalf("合并重开后不一致:\n got=%v\nwant=%v", got, wantAll)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeLegacyStoreSchemaEvolution(t *testing.T) {
	biz := "版本兼容-老库升级后结构演进"
	dir := t.TempDir()
	wantAll := buildLegacyStore(t, dir)
	ctx := context.Background()

	s := mustOpen(t, dir, DefaultOptions())

	// 1. 改名：老数据在新名字下连续可读。
	if err := s.RenameColumn(ctx, "name", "label"); err != nil {
		t.Fatal(err)
	}
	got := scanAll(t, s, ScanOptions{Columns: []string{"id", "label"}})
	want := make([]Row, len(wantAll))
	for i, r := range wantAll {
		want[i] = Row{"id": r["id"], "label": r["name"]}
	}
	logCase(t, biz, "老列改名后读", "rename name->label", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("改名后老数据不可读:\n got=%v\nwant=%v", got, want)
	}

	// 2. 新增列：老行读 NULL，新行有值。
	if err := s.AddColumn(ctx, Column{Name: "note", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	newRow := Row{"id": int64(5), "score": 5.5, "label": "eve", "tag": "y", "active": true, "note": "n5"}
	if err := s.Append(ctx, newRow); err != nil {
		t.Fatal(err)
	}
	got = scanAll(t, s, ScanOptions{Columns: []string{"id", "note"}})
	want = make([]Row, 0, len(wantAll)+1)
	for _, r := range wantAll {
		want = append(want, Row{"id": r["id"], "note": nil})
	}
	want = append(want, Row{"id": int64(5), "note": "n5"})
	logCase(t, biz, "新增列后读", "add note + append", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("新增列后读不一致:\n got=%v\nwant=%v", got, want)
	}

	// 3. 升级后的删列再加同名列：获得新语义（老值读 NULL，不错位）。
	if err := s.DropColumn(ctx, "active"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddColumn(ctx, Column{Name: "active", Type: TypeBool, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	got = scanAll(t, s, ScanOptions{Columns: []string{"id", "active"}})
	want = make([]Row, 0, len(wantAll)+1)
	for _, r := range wantAll {
		want = append(want, Row{"id": r["id"], "active": nil})
	}
	want = append(want, Row{"id": int64(5), "active": nil})
	logCase(t, biz, "升级后同名重生", "drop active + add active", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("升级后同名重生应读 NULL:\n got=%v\nwant=%v", got, want)
	}

	// 4. 类型演进：老段 int64 按 float64 读出。
	if err := s.AlterColumnType(ctx, "id", TypeFloat64); err != nil {
		t.Fatal(err)
	}
	got = scanAll(t, s, ScanOptions{Columns: []string{"id"}})
	want = make([]Row, 0, len(wantAll)+1)
	for _, r := range wantAll {
		want = append(want, Row{"id": float64(r["id"].(int64))})
	}
	want = append(want, Row{"id": float64(5)})
	logCase(t, biz, "老段类型演进", "id int64->float64", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("类型演进后读不一致:\n got=%v\nwant=%v", got, want)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 5. 演进后重开，结果仍一致。
	s2 := mustOpen(t, dir, DefaultOptions())
	got = scanAll(t, s2, ScanOptions{Columns: []string{"id", "label"}})
	want = make([]Row, 0, len(wantAll)+1)
	for _, r := range wantAll {
		want = append(want, Row{"id": float64(r["id"].(int64)), "label": r["name"]})
	}
	want = append(want, Row{"id": float64(5), "label": "eve"})
	logCase(t, biz, "演进重开后读", "reopen project id,label", got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("演进重开后不一致:\n got=%v\nwant=%v", got, want)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// ---------- 不兼容与损坏的区分 ----------

func TestUpgradeUnsupportedFutureVersion(t *testing.T) {
	biz := "版本兼容-未来版本打开即报且不伪装成损坏"
	dir := t.TempDir()
	buildLegacyStore(t, dir)

	// 把段文件头的格式版本改成未来版本号（固定头不受 CRC 保护，直接改字节）。
	segPath := filepath.Join(dir, segmentFileName(1))
	mutateFile(t, segPath, func(b []byte) []byte {
		binary.BigEndian.PutUint16(b[4:6], segmentFormatVersion+1)
		return b
	})

	_, err := Open(dir, DefaultOptions())
	logCase(t, biz, "未来版本段", "Open", err)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("打开时必须报 ErrUnsupportedVersion, got %v", err)
	}
	if errors.Is(err, ErrCorruptData) {
		t.Fatalf("不兼容不能伪装成数据损坏, got %v", err)
	}
}

func TestUpgradeCorruptStillReportedAsCorrupt(t *testing.T) {
	biz := "版本兼容-真损坏仍报损坏可与不兼容区分"
	dir := t.TempDir()
	buildLegacyStore(t, dir)

	// 翻段文件数据区一个字节（CRC 失败）：这是损坏，不是不兼容。
	segPath := filepath.Join(dir, segmentFileName(2))
	mutateFile(t, segPath, func(b []byte) []byte {
		b[len(b)-6] ^= 0xFF
		return b
	})

	s := mustOpen(t, dir, DefaultOptions()) // 打开不报错
	_, err := scanAllErr(s, ScanOptions{})
	logCase(t, biz, "老段数据区被改", "scan *", err)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("读取时必须报 ErrCorruptData, got %v", err)
	}
	if errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("损坏不能被误报成版本不兼容, got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
