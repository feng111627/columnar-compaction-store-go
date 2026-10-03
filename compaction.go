package columnar

import (
	"os"
	"path/filepath"
	"time"
)

// CompactionStats 汇总一次合并的结果。
type CompactionStats struct {
	InputFiles int    `json:"-"`
	OutputFile int64  `json:"-"`
	OutputPath string `json:"-"`
	RowsIn     int    `json:"-"`
	RowsOut    int    `json:"-"`
}

// Compact 把当前快照中的全部数据文件合并为一个更大的文件。
// 调用语义见 compactLocked；该方法负责获取串行锁。
func (t *Table) Compact() (CompactionStats, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.compactLocked()
}

// compactLocked 在持有 t.mu 时执行合并：
//
//  1. 选定输入文件（当前 manifest 的全部文件，0 或 1 个时无需合并）。
//  2. 按当前 schema 读出所有列（缺失列补 NULL、旧类型即时转换），
//     在内存拼成一个大批次。
//  3. 以“临时文件 + 原子 rename”写出新数据文件；此刻读者仍只看到旧文件集合。
//  4. beginSwap 等待在途读取结束后原子换入新 manifest。测试钩子 failBeforeSwap
//     在此刻强制失败：删除半成品、保留旧 manifest，回到合并前一致状态。
//  5. 换入成功后再物理删除旧文件（此时没有任何读者能再引用它们）。
//
// 因此并发读者看到的文件集合永远是“合并前全集”或“合并后全集”之一。
func (t *Table) compactLocked() (stats CompactionStats, err error) {
	const op = "compact"
	old := t.cur
	if len(old.Files) < 2 {
		return CompactionStats{}, failf(op, ErrInvalidArgument,
			"当前只有 %d 个数据文件，无需合并", len(old.Files))
	}

	curSchema := t.hs.Current()
	wantIDs := curSchema.ColumnIDs()
	totalRows := 0
	for _, f := range old.Files {
		totalRows += f.NumRows
	}

	readers := make([]*DataReader, 0, len(old.Files))
	for _, f := range old.Files {
		dr, derr := OpenDataFile(f.Path)
		if derr != nil {
			for _, x := range readers {
				x.Close()
			}
			return CompactionStats{}, derr
		}
		readers = append(readers, dr)
	}
	merged, merr := mergeReaders(readers, wantIDs, totalRows, old, curSchema)
	for _, dr := range readers {
		dr.Close()
	}
	if merr != nil {
		return CompactionStats{}, merr
	}

	outID := t.nextFileID + 1
	t.nextFileID = outID
	outPath := filepath.Join(t.ms.tableDir(), dataFileName(outID))
	if werr := WriteDataFile(outPath, curSchema.Version, totalRows, merged); werr != nil {
		return CompactionStats{}, werr
	}
	// 到此为止新文件对任何读者都不可见（manifest 尚未引用它）。
	rollback := true
	defer func() {
		if rollback {
			os.Remove(outPath)
		}
	}()

	entry := FileEntry{
		ID:            outID,
		Path:          outPath,
		SchemaVersion: curSchema.Version,
		NumRows:       totalRows,
		CreatedAt:     time.Now(),
	}
	m := buildManifest(old.ID+1, t.hs.Table, []FileEntry{entry}, t.hs, time.Now())

	// 进入换入窗口：等待所有在途读取结束，此时并发读者在换入完成前无法开始
	// 新读取。测试钩子在此刻强制失败，验证回滚路径。
	t.beginSwap()
	if t.failBeforeSwap {
		t.abortSwap()
		return CompactionStats{}, failf(op, ErrInvalidArgument,
			"测试注入：新文件已写出但 manifest 换入前失败（新文件 %s 将被清理）", outPath)
	}
	if serr := t.ms.Save(m); serr != nil {
		t.abortSwap()
		return CompactionStats{}, serr
	}
	t.commitSwap(m)
	_ = t.ms.RemoveOldManifests(m.ID)

	// 换入成功：此后没有新读者会再打开旧文件，而 beginSwap 已保证旧读者
	// 全部退出，因此可以安全物理删除旧文件。
	for _, f := range old.Files {
		os.Remove(f.Path)
	}
	rollback = false
	return CompactionStats{
		InputFiles: len(old.Files),
		OutputFile: outID,
		OutputPath: outPath,
		RowsIn:     totalRows,
		RowsOut:    totalRows,
	}, nil
}

// mergeReaders 按当前 schema 视角拼接多个文件的列数据。
//   - 列按 ID 对齐；文件写入时不存在的列 -> 该文件区间全 NULL。
//   - 文件记录类型旧于当前类型 -> 按结构演进兼容规则即时转换。
//   - 数据块缺失/损坏/类型不自洽 -> 返回错误，绝不丢行。
func mergeReaders(rs []*DataReader, wantIDs []int, totalRows int, old *Manifest, cur *Schema) (map[int]*Column, error) {
	const op = "compact.merge"
	out := make(map[int]*Column, totalRows)
	for _, id := range wantIDs {
		def, ok := cur.ColumnByID(id)
		if !ok {
			return nil, failf(op, ErrColumnNotFound, "当前 schema 缺少列 ID %d", id)
		}
		out[id] = newAppendColumn(def.Name, def.Type, totalRows)
	}

	for _, dr := range rs {
		fileSchema, err := schemaAtVersion(old, dr.SchemaVersion())
		if err != nil {
			return nil, err
		}
		n := dr.NumRows()
		for _, id := range wantIDs {
			def := cur.byID[id]
			dst := out[id]
			fileDef, existed := fileSchema.ColumnByID(id)
			if !existed {
				dst.appendNulls(n)
				continue
			}
			if !dr.HasColumn(id) {
				return nil, failf(op, ErrCorruptData,
					"文件 %s 的 schema(v%d) 声明了列 %q(id=%d) 但数据块缺失",
					dr.path, dr.SchemaVersion(), fileDef.Name, id)
			}
			fileType, col, rerr := dr.ReadColumnByID(id)
			if rerr != nil {
				return nil, rerr
			}
			if fileType != fileDef.Type {
				return nil, failf(op, ErrCorruptData,
					"文件 %s 列 %q(id=%d) 块类型 %s 与 schema(v%d) 的 %s 不一致",
					dr.path, fileDef.Name, id, fileType, dr.SchemaVersion(), fileDef.Type)
			}
			col.Name = def.Name
			if fileType != def.Type {
				conv, cerr := convertColumn(col, def.Type)
				if cerr != nil {
					return nil, cerr
				}
				col = conv
			}
			dst.appendColumn(col)
		}
	}
	for _, c := range out {
		if err := c.Valid(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func newAppendColumn(name string, t ValueType, capRows int) *Column {
	c := &Column{Name: name, Type: t, Nulls: make([]bool, 0, capRows)}
	switch t {
	case TypeInt64:
		c.I64s = make([]int64, 0, capRows)
	case TypeFloat64:
		c.F64s = make([]float64, 0, capRows)
	case TypeBool:
		c.Bools = make([]bool, 0, capRows)
	case TypeString:
		c.Strs = make([]string, 0, capRows)
	}
	return c
}

func (c *Column) appendValue(v Value) {
	c.Nulls = append(c.Nulls, !v.Valid)
	switch c.Type {
	case TypeInt64:
		c.I64s = append(c.I64s, v.I64)
	case TypeFloat64:
		c.F64s = append(c.F64s, v.F64)
	case TypeBool:
		c.Bools = append(c.Bools, v.Bool)
	case TypeString:
		c.Strs = append(c.Strs, v.Str)
	}
}

func (c *Column) appendNulls(n int) {
	null := NullValue(c.Type)
	for i := 0; i < n; i++ {
		c.appendValue(null)
	}
}

func (c *Column) appendColumn(other *Column) {
	for r := 0; r < other.Rows(); r++ {
		c.appendValue(other.ValueAt(r))
	}
}
