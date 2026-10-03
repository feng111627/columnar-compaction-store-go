package columnar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Append 追加一行（按当前结构校验类型与必填），达到 FlushThreshold 自动落段。
func (s *Store) Append(ctx context.Context, row Row) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	normalized, err := validateRow(*s.m.currentSchema(), row)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.buf = append(s.buf, normalized)
	if len(s.buf) >= s.opt.FlushThreshold {
		return s.flushLocked(ctx)
	}
	return nil
}

// Flush 把内存数据显式刷成段文件。
func (s *Store) Flush(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return s.flushLocked(ctx)
}

// validateRow 按结构校验一行并产出规范化行（只保留声明列）：
//   - 行里出现未声明列：ErrColumnNotFound（不静默丢字段）；
//   - 值类型与列类型不符：ErrTypeMismatch；
//   - 缺列当作 NULL；非 NULL 约束列为 NULL：ErrTypeMismatch。
func validateRow(schema Schema, row Row) (Row, error) {
	out := make(Row, len(schema.Columns))
	for _, col := range schema.Columns {
		v, present := row[col.Name]
		if !present || v == nil {
			if !col.Nullable {
				return nil, fmt.Errorf("%w: column %q is not nullable but got NULL",
					ErrTypeMismatch, col.Name)
			}
			out[col.Name] = nil
			continue
		}
		if !goTypeMatches(col.Type, v) {
			return nil, fmt.Errorf("%w: column %q expects %s, got %T",
				ErrTypeMismatch, col.Name, col.Type, v)
		}
		out[col.Name] = v
	}
	for k := range row {
		if _, ok := schema.Column(k); !ok {
			return nil, fmt.Errorf("%w: row has value for undeclared column %q",
				ErrColumnNotFound, k)
		}
	}
	return out, nil
}

func goTypeMatches(t DataType, v any) bool {
	switch t {
	case TypeInt64:
		_, ok := v.(int64)
		return ok
	case TypeFloat64:
		_, ok := v.(float64)
		return ok
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	default:
		return false
	}
}

// flushLocked 把缓冲写成新段并登记进清单。失败时清理临时文件，
// 内存缓冲保留不动，调用方可重试；任何情况下磁盘状态都保持一致。
func (s *Store) flushLocked(ctx context.Context) error {
	if len(s.buf) == 0 {
		return nil
	}
	schema := *s.m.currentSchema()
	id := s.m.NextSegmentID
	sw, err := newSegmentWriter(s.dir, id, schema)
	if err != nil {
		return err
	}
	created := true
	defer func() {
		if created {
			_ = sw.Close()
			_ = os.Remove(sw.tmpPath())
		}
	}()

	for _, r := range s.buf {
		if err := sw.appendRow(r); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	finalName, n, err := sw.commit(s.m.Version)
	if err != nil {
		return err
	}

	segs := make([]segmentMeta, 0, len(s.m.Segments)+1)
	segs = append(segs, s.m.Segments...)
	segs = append(segs, segmentMeta{
		ID:            id,
		FileName:      finalName,
		RowCount:      n,
		SchemaVersion: schema.Version,
		Stats:         buildSegmentStats(s.buf, schema),
	})
	cand := &manifest{
		Version:       s.m.Version + 1,
		SchemaHistory: s.m.SchemaHistory,
		Segments:      segs,
		NextSegmentID: id + 1,
		NextColumnID:  s.m.NextColumnID,
	}
	if err := saveManifest(s.dir, cand); err != nil {
		_ = os.Remove(filepath.Join(s.dir, finalName))
		return err
	}
	s.m = cand
	s.buf = nil
	created = false
	return sw.Close()
}
