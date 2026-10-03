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
	ID            int64  `json:"id"`
	FileName      string `json:"file_name"`
	RowCount      int    `json:"row_count"`
	SchemaVersion int    `json:"schema_version"` // 写入该段时使用的结构版本
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

// resolveStoredColumn 把段文件里存储的列名映射到当前结构中的列。
// seg 是段写入时的结构快照（nil 表示没有历史信息）。
//
// 规则：
//   - 段写入时结构里该列带生命周期 ID：只认 ID。当前结构中同 ID 的列才是
//     同一列（改名后 ID 不变，历史数据在新名字下连续可读）；当前结构没有
//     同 ID 列，说明它已被删除、或名字被另一个新列占用（同名重生），
//     这些物理字节对当前读取一律不可见。
//   - 无身份信息（ID 为 0，仅出现在手工构造的清单里）：退回按名称匹配。
func resolveStoredColumn(seg *Schema, cur Schema, storedName string) (Column, bool) {
	if seg != nil {
		if sc, ok := seg.Column(storedName); ok && sc.ID != 0 {
			return cur.columnByID(sc.ID)
		}
	}
	return cur.Column(storedName)
}

// storedNameFor 反向映射：当前列名 -> 该段文件里存储时使用的名字。
// 段写入时还没有这一列（或身份信息缺失时退化为同名）时按名称返回。
func storedNameFor(seg *Schema, cur Schema, curName string) string {
	if seg != nil {
		if cc, ok := cur.Column(curName); ok && cc.ID != 0 {
			if sc, ok := seg.columnByID(cc.ID); ok {
				return sc.Name
			}
			return "" // 段写入时该列尚不存在
		}
	}
	return curName
}

// segmentReader 只读打开一个段文件，读取按当前（可能更新过的）结构进行。
type segmentReader struct {
	f      *os.File
	dec    *segmentDecoder
	schema Schema
	seg    *Schema // 段写入时的结构快照（列身份解析用），可为 nil
	stored segmentFileHeader
}

// openSegment 校验头部/目录后打开段文件。hist 为结构历史（列身份解析用），
// 传 nil 时退化为纯按名称匹配。
func openSegment(dir string, meta segmentMeta, target Schema, hist []Schema) (*segmentReader, error) {
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
	return openSegmentReaderAt(f, fi.Size(), meta, target, hist)
}

func openSegmentReaderAt(ra io.ReaderAt, size int64, meta segmentMeta, target Schema, hist []Schema) (*segmentReader, error) {
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
	var seg *Schema
	if sch, ok := schemaAtVersion(hist, meta.SchemaVersion); ok {
		sch := sch
		seg = &sch
	}
	return &segmentReader{dec: dec, schema: target, seg: seg, stored: h}, nil
}

// scan 段内列裁剪 + 过滤下推。存储类型与当前结构不兼容时返回
// ErrIncompatibleSchema，字节损坏返回 ErrCorruptData。
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

	// 段级跳过：统计块可用且某个过滤条件对本段所有行必然为假时，
	// 整段一个数据帧都不读。统计缺失/损坏时 loadStats 返回 ok=false，
	// 自动退回完整扫描。
	if len(opts.Filters) > 0 && r.definitelyNoMatch(opts.Filters) {
		return []Row{}, nil
	}

	cols, err := r.dec.loadColumns(r.schema, r.seg, wanted)
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
				row[c.Name] = nil // 段写入后才新增的列
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

// definitelyNoMatch 依据统计块判断"该段是否必然没有任何行命中全部过滤条件"
// （AND 语义：任一条件必然为假即可整段跳过）。任何不确定都返回 false。
func (r *segmentReader) definitelyNoMatch(filters []Filter) bool {
	stats, ok := r.dec.loadStats()
	if !ok {
		return false // 统计缺失/损坏：退回完整扫描
	}
	for _, f := range filters {
		stored := storedNameFor(r.seg, r.schema, f.Column)
		if stored == "" {
			// 段写入时该列尚不存在：本段该列全 NULL，任何比较都不命中。
			return true
		}
		if !statsMayMatch(stats, stored, f) {
			return true
		}
	}
	return false
}

func (r *segmentReader) close() error {
	if r.f != nil {
		return r.f.Close()
	}
	return nil
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
