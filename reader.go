package columnar

import (
	"context"
	"fmt"
)

// snapshotScanner 一致快照扫描器：构造时冻结段清单、结构版本与内存缓冲，
// 并在 store.readers 登记引用；活动期间 compaction 不会删除它读到的旧段。
type snapshotScanner struct {
	store  *Store
	schema Schema
	metas  []segmentMeta
	buf    []Row // 快照时刻尚未落盘的内存行
	opts   ScanOptions

	rows []Row // 延迟解码后的结果
	idx  int

	loaded bool
	err    error
	closed bool
}

// Scan 打开一个一致快照扫描器，支持列裁剪与过滤下推。
func (s *Store) Scan(ctx context.Context, opts ScanOptions) (Scanner, error) {
	if err := s.checkOpen(); err != nil {
		return nil, err
	}
	if err := s.validateScanOptions(opts); err != nil {
		return nil, err
	}
	metas, schema, buf := s.pinSnapshot()
	sc := &snapshotScanner{
		store:  s,
		schema: schema,
		metas:  metas,
		buf:    buf,
		opts:   opts,
	}
	return sc, nil
}

func (s *Store) validateScanOptions(opts ScanOptions) error {
	s.mu.RLock()
	schema := *s.m.currentSchema()
	s.mu.RUnlock()
	if _, _, err := resolveProjection(schema, opts); err != nil {
		return err
	}
	return validateFilters(schema, opts.Filters)
}

func (sc *snapshotScanner) load(ctx context.Context) error {
	project := len(sc.opts.Columns) > 0
	needed := map[string]bool{}
	if project {
		for _, c := range sc.opts.Columns {
			needed[c] = true
		}
	} else {
		for _, c := range sc.schema.Columns {
			needed[c.Name] = true
		}
	}
	if err := validateFilters(sc.schema, sc.opts.Filters); err != nil {
		return err
	}

	var out []Row
	// 先按段写入先后顺序读磁盘段，再读内存快照行（未刷盘的是最新写入的行）。
	for _, meta := range sc.metas {
		if err := ctx.Err(); err != nil {
			return err
		}
		rd, err := openSegment(sc.store.dir, meta, sc.schema)
		if err != nil {
			return err
		}
		segRows, err := rd.scan(ctx, sc.opts)
		_ = rd.close()
		if err != nil {
			return err
		}
		out = append(out, segRows...)
	}
	for _, r := range sc.buf {
		keep, err := filtersMatch(r, sc.opts.Filters)
		if err != nil {
			return err
		}
		if keep {
			out = append(out, projectRow(r, project, sc.opts.Columns))
		}
	}
	sc.rows = out
	sc.loaded = true
	return nil
}

func (sc *snapshotScanner) Next() (Row, error) {
	if sc.closed {
		return nil, ErrClosed
	}
	if sc.err != nil {
		return nil, sc.err
	}
	if !sc.loaded {
		sc.err = sc.load(context.Background())
		if sc.err != nil {
			return nil, sc.err
		}
	}
	if sc.idx >= len(sc.rows) {
		return nil, fmt.Errorf("no more rows: %w", errEOF())
	}
	r := sc.rows[sc.idx]
	sc.idx++
	return r, nil
}

func (sc *snapshotScanner) Close() error {
	if sc.closed {
		return nil
	}
	sc.closed = true
	sc.store.releaseReader()
	return nil
}

// errEOF 返回哨兵 io.EOF（单独函数避免在文件顶部到处 import io）。
func errEOF() error { return ioEOF }

// filtersMatch 对一行求值全部过滤条件（AND 语义）。
func filtersMatch(row Row, fs []Filter) (bool, error) {
	for _, f := range fs {
		ok, err := matchFilter(row, f)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// matchFilter 对一行求值单个过滤条件。缺失列/NULL/类型异常都是明确错误。
func matchFilter(row Row, f Filter) (bool, error) {
	v, ok := row[f.Column]
	if !ok {
		return false, fmt.Errorf("%w: filter column %q absent from row", ErrColumnNotFound, f.Column)
	}
	if v == nil {
		return false, nil // NULL 对任何比较都不为真
	}
	cmp, err := compareValues(v, f.Value)
	if err != nil {
		return false, err
	}
	switch f.Op {
	case OpEqual:
		return cmp == 0, nil
	case OpNotEqual:
		return cmp != 0, nil
	case OpLess:
		return cmp < 0, nil
	case OpLessEqual:
		return cmp <= 0, nil
	case OpGreater:
		return cmp > 0, nil
	case OpGreaterEqual:
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("%w: unknown filter operator %d", ErrTypeMismatch, f.Op)
	}
}

// compareValues 返回 -1/0/1。比较双方必须同类型（过滤值已按列类型校验；
// 经 int64->float64 演进读出的值同为 float64）。
func compareValues(a, b any) (int, error) {
	switch x := a.(type) {
	case int64:
		y, ok := b.(int64)
		if !ok {
			return 0, fmt.Errorf("%w: cannot compare int64 with %T", ErrTypeMismatch, b)
		}
		switch {
		case x < y:
			return -1, nil
		case x > y:
			return 1, nil
		default:
			return 0, nil
		}
	case float64:
		y, ok := b.(float64)
		if !ok {
			return 0, fmt.Errorf("%w: cannot compare float64 with %T", ErrTypeMismatch, b)
		}
		switch {
		case x < y:
			return -1, nil
		case x > y:
			return 1, nil
		default:
			return 0, nil
		}
	case string:
		y, ok := b.(string)
		if !ok {
			return 0, fmt.Errorf("%w: cannot compare string with %T", ErrTypeMismatch, b)
		}
		switch {
		case x < y:
			return -1, nil
		case x > y:
			return 1, nil
		default:
			return 0, nil
		}
	case bool:
		y, ok := b.(bool)
		if !ok {
			return 0, fmt.Errorf("%w: cannot compare bool with %T", ErrTypeMismatch, b)
		}
		if x == y {
			return 0, nil
		}
		if !x {
			return -1, nil
		}
		return 1, nil
	default:
		return 0, fmt.Errorf("%w: cannot compare value of type %T", ErrTypeMismatch, a)
	}
}

// projectRow 按投影裁剪。project=false 时原样返回。
func projectRow(row Row, project bool, cols []string) Row {
	if !project {
		return row
	}
	out := make(Row, len(cols))
	for _, c := range cols {
		out[c] = row[c]
	}
	return out
}
