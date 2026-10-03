package columnar

import (
	"context"
	"testing"
)

// countingReaderAt 统计发生了多少次 ReadAt 以及读取的字节范围覆盖。
type countingReaderAt struct {
	data   []byte
	calls  int
	bytes  int
	ranges [][2]int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	c.calls++
	c.bytes += len(p)
	c.ranges = append(c.ranges, [2]int64{off, off + int64(len(p))})
	n := copy(p, c.data[off:])
	if n == len(p) {
		return n, nil
	}
	return n, errEOF()
}

func TestColumnPruningIsByteLevel(t *testing.T) {
	biz := "列式读取-字节级列裁剪"
	_, segPath := buildOneSegmentStore(t)
	raw := readFileBytes(t, segPath)

	meta := segmentMeta{FileName: "inmem", RowCount: 2}
	schema := testSchema2()

	run := func(name string, opts ScanOptions) *countingReaderAt {
		cr := &countingReaderAt{data: raw}
		rd, err := openSegmentReaderAt(cr, int64(len(raw)), meta, schema, nil)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := rd.scan(context.Background(), opts)
		_ = rd.close()
		if err != nil {
			t.Fatal(err)
		}
		var cols []string
		for _, r := range rows {
			cols = append(cols, keysOf(r)...)
		}
		logCase(t, biz, name, len(cols), map[string]any{"readCalls": cr.calls, "readBytes": cr.bytes, "fileBytes": len(raw)})
		return cr
	}

	// 用 openDecoder 解析出各帧偏移，判断读取范围是否触碰了 score/name/active 帧。
	_, h, err := openDecoder(&sliceReaderAt{raw}, int64(len(raw)))
	if err != nil {
		t.Fatal(err)
	}
	frame := map[string][2]int64{}
	for _, e := range h.dir {
		frame[e.name] = [2]int64{e.offset, e.offset + int64(e.frameLen)}
	}

	touches := func(cr *countingReaderAt, col string) bool {
		r := frame[col]
		for _, rg := range cr.ranges {
			if rg[0] < r[1] && rg[1] > r[0] {
				return true
			}
		}
		return false
	}

	full := run("读全部列(4列)", ScanOptions{})
	one := run("只读id列", ScanOptions{Columns: []string{"id"}})
	withFilter := run("投影name并按id过滤", ScanOptions{
		Columns: []string{"name"},
		Filters: []Filter{{Column: "id", Op: OpEqual, Value: int64(1)}},
	})

	if one.bytes >= full.bytes {
		t.Fatalf("pruning did not reduce bytes read: pruned=%d full=%d", one.bytes, full.bytes)
	}
	for _, col := range []string{"score", "name", "active"} {
		if touches(one, col) {
			t.Fatalf("projecting id only must not read column %q frame", col)
		}
	}
	if !touches(withFilter, "id") || !touches(withFilter, "name") {
		t.Fatal("filter column id and projected name must be read")
	}
	if touches(withFilter, "score") || touches(withFilter, "active") {
		t.Fatal("score and active frames must not be touched")
	}
	t.Logf("[业务=%s] 结论: 全列读=%d字节, 单列读=%d字节, 过滤组合读=%d字节（文件=%d字节）",
		biz, full.bytes, one.bytes, withFilter.bytes, len(raw))
}

// sliceReaderAt 仅用于解析帧布局。
type sliceReaderAt struct{ b []byte }

func (s *sliceReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n := copy(p, s.b[off:])
	if n == len(p) {
		return n, nil
	}
	return n, errEOF()
}
