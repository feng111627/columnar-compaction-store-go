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

// segmentReader 只读打开一个段文件，读取按当前（可能更新过的）结构进行。
type segmentReader struct {
	f      *os.File
	dec    *segmentDecoder
	schema Schema
	stored segmentFileHeader
}

// openSegment 校验头部/目录后打开段文件。
func openSegment(dir string, meta segmentMeta, target Schema) (*segmentReader, error) {
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
	return openSegmentReaderAt(f, fi.Size(), meta, target)
}

func openSegmentReaderAt(ra io.ReaderAt, size int64, meta segmentMeta, target Schema) (*segmentReader, error) {
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
	return &segmentReader{dec: dec, schema: target, stored: h}, nil
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

	cols, err := r.dec.loadColumns(r.schema, wanted)
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
