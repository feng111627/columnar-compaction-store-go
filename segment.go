package columnar

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// segmentMeta 清单中记录的一个段文件。
type segmentMeta struct {
	ID            int64                 `json:"id"`
	FileName      string                `json:"file_name"`
	RowCount      int                   `json:"row_count"`
	SchemaVersion int                   `json:"schema_version"`  // 写入该段时使用的结构版本
	Stats         map[string]columnStat `json:"stats,omitempty"` // 段级 min/max 依据（按段写入时列名）
}

// columnStat 一列在一个段内的极值依据，用于带过滤条件扫描时整段跳过。
// 空段或全 NULL 段：NullCount==RowCount 时极值字段为零值，跳过判定另走
// 专门分支。统计随数据一起在段生成时计算并随清单原子持久化；合并时对
// 合并数据重新计算，因此依据永远跟着数据走。
//
// CRC 是对除自身外其余字段规范化 JSON 编码的 crc32-IEEE，在段生成时计算。
// 这样清单字节损坏（翻字节/截断/手改）导致的依据失真会在"跳过决策"时就
// 被识别为不可信依据而拒绝跳过，无需读取段文件即可保证不会错误漏行。
//
// 一旦依据缺失（map/条目为空、身份无法绑定）、CRC 损坏、或读到真实数据
// 后重算的极值与依据对不上，读取一律安全回退为完整扫描——宁可多读，绝不
// 少行。
type columnStat struct {
	Type      DataType `json:"type"`
	NullCount int      `json:"null_count"`
	// 四类极值用各自类型的字段表达，保证 JSON 往返精度无损（避免 float64
	// 吞没 int64 的问题）。只有与 Type 对应的字段有意义。
	IntV    int64   `json:"int_min,omitempty"`
	IntMax  int64   `json:"int_max,omitempty"`
	FltV    float64 `json:"float_min,omitempty"`
	FltMax  float64 `json:"float_max,omitempty"`
	StrMin  string  `json:"str_min,omitempty"`
	StrMax  string  `json:"str_max,omitempty"`
	BoolMin bool    `json:"bool_min,omitempty"`
	BoolMax bool    `json:"bool_max,omitempty"`
	CRC     uint32  `json:"crc"`
}

// segmentWriter 列式数据文件写入器：在内存攒行，Commit 时一次性编码、
// 写到 .tmp 文件并 fsync，再原子 rename 成正式段文件。
type segmentWriter struct {
	dir    string
	id     int64
	schema Schema

	tmp *os.File

	rows []Row
	done bool
}

// newSegmentWriter 创建待提交的临时段文件。
func newSegmentWriter(dir string, id int64, schema Schema) (*segmentWriter, error) {
	tmpPath := filepath.Join(dir, segmentTempName(id))
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &segmentWriter{dir: dir, id: id, schema: schema, tmp: f}, nil
}

func (w *segmentWriter) tmpPath() string {
	if w.tmp == nil {
		return filepath.Join(w.dir, segmentTempName(w.id))
	}
	return w.tmp.Name()
}

func (w *segmentWriter) appendRow(row Row) error {
	if w.done {
		return fmt.Errorf("columnar: writer already committed")
	}
	w.rows = append(w.rows, row)
	return nil
}

// commit 编码全部行 -> 写临时文件 -> fsync -> rename 为正式段。
// manifestVersion 仅用于校验当前写入基于的清单版本。
func (w *segmentWriter) commit(manifestVersion int) (string, int, error) {
	_ = manifestVersion
	if w.done {
		return "", 0, fmt.Errorf("columnar: writer already committed")
	}
	data, err := encodeSegment(w.schema.Version, w.rows, w.schema)
	if err != nil {
		return "", 0, err
	}
	if _, err := w.tmp.Write(data); err != nil {
		return "", 0, err
	}
	if err := w.tmp.Sync(); err != nil {
		return "", 0, err
	}
	if err := w.tmp.Close(); err != nil {
		return "", 0, err
	}
	finalName := segmentFileName(w.id)
	if err := os.Rename(w.tmp.Name(), filepath.Join(w.dir, finalName)); err != nil {
		return "", 0, err
	}
	if df, err := os.Open(w.dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	w.done = true
	return finalName, len(w.rows), nil
}

// Close 释放句柄；未 Commit 时由调用方负责删除临时文件。
func (w *segmentWriter) Close() error {
	if w.tmp == nil || w.done {
		return nil
	}
	err := w.tmp.Close()
	w.tmp = nil
	return err
}

// segmentReader 只读打开一个段文件，读取按当前（可能更新过的）结构进行。
type segmentReader struct {
	f           *os.File
	dec         *segmentDecoder
	schema      Schema // 当前结构（扫描结果以此为准）
	writeSchema Schema // 该段写入时使用的结构（用于列身份绑定）
	stored      segmentFileHeader
}

// openSegment 校验头部/目录后打开段文件。writeSchema 是该段写入时的结构
// （由清单的 SchemaVersion 在结构历史中查出），用于把物理列帧按稳定身份
// 绑定到当前结构列。
func openSegment(dir string, meta segmentMeta, writeSchema, target Schema) (*segmentReader, error) {
	path := filepath.Join(dir, meta.FileName)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return openSegmentReaderAt(f, fi.Size(), meta, writeSchema, target)
}

func openSegmentReaderAt(ra io.ReaderAt, size int64, meta segmentMeta, writeSchema, target Schema) (*segmentReader, error) {
	dec, h, err := openDecoder(ra, size)
	if err != nil {
		if f, ok := ra.(*os.File); ok {
			_ = f.Close()
		}
		return nil, fmt.Errorf("segment %s: %w", meta.FileName, err)
	}
	if h.rowCount != meta.RowCount {
		if f, ok := ra.(*os.File); ok {
			_ = f.Close()
		}
		return nil, fmt.Errorf("%w: segment %s header rows %d != manifest rows %d",
			ErrCorruptData, meta.FileName, h.rowCount, meta.RowCount)
	}
	return &segmentReader{dec: dec, schema: target, writeSchema: writeSchema, stored: h}, nil
}

// scan 段内列裁剪 + 过滤下推。存储类型与当前结构不兼容时返回
// ErrIncompatibleSchema，字节损坏返回 ErrCorruptData。
//
// 物理列帧先经 resolveFrameBindings 绑定到当前结构列：
//   - 改名列：旧名帧绑定到新名字，历史值连续可读；
//   - 同名重生列：旧帧绑定失败，整列读成 NULL，不解释其字节，
//     因此新旧类型不兼容也只是这一列为空，整表照常可读；
//   - 已删除列：绑定失败，不加载也不出现在结果中。
func (r *segmentReader) scan(ctx context.Context, opts ScanOptions) ([]Row, error) {
	wanted, project, err := resolveProjection(r.schema, opts)
	if err != nil {
		return nil, err
	}
	if err := validateFilters(r.schema, opts.Filters); err != nil {
		return nil, err
	}
	for _, f := range opts.Filters {
		wanted[f.Column] = true
	}

	bindings := resolveFrameBindings(r.dec.header().dir, r.writeSchema, r.schema)
	cols, err := r.dec.loadColumns(bindings, wanted)
	if err != nil {
		return nil, err
	}

	n := r.dec.rowCount()
	rows := make([]Row, 0, n)
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row := make(Row, len(r.schema.Columns))
		for _, c := range r.schema.Columns {
			if !wanted[c.Name] {
				continue
			}
			if dc, ok := cols[c.Name]; ok {
				row[c.Name] = dc.vals[i]
			} else {
				row[c.Name] = nil // 段写入后才新增的列，或同名重生前的旧身份帧
			}
		}
		keep := true
		for _, f := range opts.Filters {
			ok, err := matchFilter(row, f)
			if err != nil {
				return nil, err
			}
			if !ok {
				keep = false
				break
			}
		}
		if !keep {
			continue
		}
		rows = append(rows, projectRow(row, project, opts.Columns))
	}
	return rows, nil
}

func (r *segmentReader) close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
}

// scanWithStatsCheck 与 scan 相同地执行列裁剪 + 过滤下推，但在读出真实
// 数据后，会用过滤涉及列在整段上的真实值重算极值，与清单中的段级依据逐列
// 精确比对。checkStats 为 true 且发现依据 CRC 损坏、身份映射缺失、类型
// 不自洽或极值/NULL 计数对不上时，返回包装了 errStatMismatch 的错误，
// 调用方据此放弃本轮（可能发生过整段跳过的）结果并完整重扫。
func (r *segmentReader) scanWithStatsCheck(ctx context.Context, opts ScanOptions, meta segmentMeta, statsKey map[string]string, checkStats bool) ([]Row, error) {
	rows, err := r.scan(ctx, opts)
	if err != nil {
		return nil, err
	}
	if !checkStats || len(opts.Filters) == 0 || meta.Stats == nil {
		return rows, nil
	}

	bindings := resolveFrameBindings(r.dec.header().dir, r.writeSchema, r.schema)
	// 过滤列即使未投影也会被完整加载；这里复用绑定结果，把每个过滤列的整段
	// 真实值取出来重算依据（不经过滤、不经投影），与清单依据精确比对。
	filterWanted := make(map[string]bool, len(opts.Filters))
	for _, f := range opts.Filters {
		filterWanted[f.Column] = true
	}
	cols, err := r.dec.loadColumns(bindings, filterWanted)
	if err != nil {
		return nil, err
	}

	for _, f := range opts.Filters {
		key, mapped := statsKey[f.Column]
		if !mapped {
			continue // 依据侧无法绑定身份：跳过判定本来就不会用到它
		}
		st, have := meta.Stats[key]
		if !have {
			continue // 无依据：跳过判定也不会用到
		}
		tcol, ok := r.schema.Column(f.Column)
		if !ok {
			return nil, errStatMismatchf("filter column %q vanished from schema", f.Column)
		}
		if !statIntact(st) {
			return nil, errStatMismatchf("stat CRC for %q is corrupt", key)
		}
		if !statTypeMatches(st, tcol, r.writeSchema, key) {
			return nil, errStatMismatchf("stat for %q type %s incompatible with current %s",
				key, st.Type, tcol.Type)
		}
		if st.NullCount < 0 || st.NullCount > meta.RowCount {
			return nil, errStatMismatchf("stat null_count %d out of range for %d rows",
				st.NullCount, meta.RowCount)
		}

		// 用真实数据重算该列依据。
		dc, present := cols[f.Column]
		wantStat, calcErr := recomputeStat(meta.RowCount, st.Type, dc, present, tcol)
		if calcErr != nil {
			return nil, calcErr
		}
		// int64 依据 + 当前 float64：把真实（已提升的 float64）值按 int64
		// 口径不可比，改为把 int 依据提升成 float64 后比较。
		if !statsEqual(st, wantStat, tcol) {
			return nil, errStatMismatchf("stat for %q disagrees with data (manifest=%+v recomputed=%+v)",
				key, st, wantStat)
		}
	}
	return rows, nil
}

// recomputeStat 用整段真实值重算一列依据。dc 为 nil/present=false 表示
// 段内没有该身份的物理帧（后增列 / 同名重生列）。
func recomputeStat(rowCount int, storedType DataType, dc *decodedColumn, present bool, tcol Column) (columnStat, error) {
	st := columnStat{Type: storedType}
	if !present {
		// 无物理帧：真实值全部为 NULL。
		st.NullCount = rowCount
		st.CRC = statCRC(st)
		return st, nil
	}
	if dc == nil || len(dc.vals) != rowCount {
		return st, errStatMismatchf("decoded column %q length wrong", tcol.Name)
	}
	switch tcol.Type {
	case TypeInt64:
		return computeStatFromVals[int64](dc.vals, TypeInt64), nil
	case TypeFloat64:
		if storedType == TypeInt64 {
			// 物理上是 int64、读取时已提升 float64；按物理口径无法直接还原
			// int 极值，这里统一按 float64 口径比较（statsEqual 特殊处理）。
			return computeStatFromVals[float64](dc.vals, TypeInt64), nil
		}
		return computeStatFromVals[float64](dc.vals, TypeFloat64), nil
	case TypeString:
		return computeStatFromVals[string](dc.vals, TypeString), nil
	case TypeBool:
		return computeStatFromVals[bool](dc.vals, TypeBool), nil
	default:
		return st, errStatMismatchf("cannot recompute stat for type %s", tcol.Type)
	}
}

func computeStatFromVals[T int64 | float64 | string | bool](vals []any, typ DataType) columnStat {
	st := columnStat{Type: typ}
	switch any(*new(T)).(type) {
	case int64:
		min, max, seen := int64(0), int64(0), false
		for _, v := range vals {
			if v == nil {
				st.NullCount++
				continue
			}
			n := v.(int64)
			if !seen {
				min, max, seen = n, n, true
				continue
			}
			if n < min {
				min = n
			}
			if n > max {
				max = n
			}
		}
		st.IntV, st.IntMax = min, max
	case float64:
		min, max, seen := 0.0, 0.0, false
		for _, v := range vals {
			if v == nil {
				st.NullCount++
				continue
			}
			f := v.(float64)
			if !seen {
				min, max, seen = f, f, true
				continue
			}
			if f < min {
				min = f
			}
			if f > max {
				max = f
			}
		}
		if typ == TypeFloat64 {
			st.FltV, st.FltMax = min, max
		} else {
			// int64 存储被提升为 float64 读取：折算回 int64 极值存到 int 字段。
			st.IntV, st.IntMax = int64(min), int64(max)
		}
	case string:
		min, max, seen := "", "", false
		for _, v := range vals {
			if v == nil {
				st.NullCount++
				continue
			}
			s := v.(string)
			if !seen {
				min, max, seen = s, s, true
				continue
			}
			if s < min {
				min = s
			}
			if s > max {
				max = s
			}
		}
		st.StrMin, st.StrMax = min, max
	case bool:
		min, max, seen := false, false, false
		for _, v := range vals {
			if v == nil {
				st.NullCount++
				continue
			}
			b := v.(bool)
			if !seen {
				min, max, seen = b, b, true
				continue
			}
			if !b {
				min = false
			}
			if b {
				max = true
			}
		}
		st.BoolMin, st.BoolMax = min, max
	}
	st.CRC = statCRC(st)
	return st
}

// statsEqual 比较清单依据与真实数据重算依据（CRC 不在比较范围内）。
func statsEqual(a, b columnStat, tcol Column) bool {
	if a.NullCount != b.NullCount {
		return false
	}
	if tcol.Type == TypeFloat64 && a.Type == TypeInt64 && b.Type == TypeInt64 {
		// int64 物理极值被提升后比较
		return a.IntV == b.IntV && a.IntMax == b.IntMax
	}
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TypeInt64:
		return a.IntV == b.IntV && a.IntMax == b.IntMax
	case TypeFloat64:
		return a.FltV == b.FltV && a.FltMax == b.FltMax
	case TypeString:
		return a.StrMin == b.StrMin && a.StrMax == b.StrMax
	case TypeBool:
		return a.BoolMin == b.BoolMin && a.BoolMax == b.BoolMax
	}
	return false
}

// statTypeMatches 核验依据类型与当前列类型的可比性（允许 int64 依据对应
// 已提升为 float64 的当前列，其余跨类型一律视为不自洽）。
func statTypeMatches(st columnStat, tcol Column, writeSchema Schema, writeName string) bool {
	if st.Type == tcol.Type {
		return true
	}
	if st.Type == TypeInt64 && tcol.Type == TypeFloat64 {
		wc, ok := writeSchema.Column(writeName)
		return ok && wc.Type == TypeInt64
	}
	return false
}

func errStatMismatchf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errStatMismatch, fmt.Sprintf(format, args...))
}

// resolveProjection 返回真正需要从磁盘加载的列集合，以及投影标志。
func resolveProjection(schema Schema, opts ScanOptions) (map[string]bool, bool, error) {
	wanted := map[string]bool{}
	if len(opts.Columns) == 0 {
		for _, c := range schema.Columns {
			wanted[c.Name] = true
		}
		return wanted, false, nil
	}
	for _, name := range opts.Columns {
		if _, ok := schema.Column(name); !ok {
			return nil, false, fmt.Errorf("%w: cannot scan unknown column %q",
				ErrColumnNotFound, name)
		}
		wanted[name] = true
	}
	return wanted, true, nil
}

func validateFilters(schema Schema, filters []Filter) error {
	for _, f := range filters {
		col, ok := schema.Column(f.Column)
		if !ok {
			return fmt.Errorf("%w: filter on unknown column %q", ErrColumnNotFound, f.Column)
		}
		if f.Value == nil {
			return fmt.Errorf("%w: filter value on column %q must not be NULL",
				ErrTypeMismatch, f.Column)
		}
		if !goTypeMatches(col.Type, f.Value) {
			return fmt.Errorf("%w: filter on %q expects %s, got %T",
				ErrTypeMismatch, f.Column, col.Type, f.Value)
		}
	}
	return nil
}
