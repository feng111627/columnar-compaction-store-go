package columnar

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// Table 是本地列式存储的句柄，一个目录对应一张表。
//
// 并发模型：
//   - mu   串行化所有会推进 manifest 的操作（Append/DDL/Compact）。
//   - 读取器创建时获取一份不可变 manifest 快照引用（计数 w），期间该快照
//     涉及的数据文件保证不被物理删除。
//   - 合并换入新 manifest 前必须等待所有在途读取结束（w==0）；等待期间
//     暂停新读取开始。换入与文件可见性在 mu+mmu 保护下原子完成，因此
//     并发读取永远看不到“合并到一半”的文件集合。
type Table struct {
	dir string

	mu   sync.Mutex // 元数据推进串行化
	mmu  sync.Mutex
	cond sync.Cond
	cur  *Manifest
	hs   *Schemas
	ms   *manifestStore

	w              int  // 持快照在途读取数
	swapInProgress bool // 换入等待期：新读取需等待
	closed         bool

	nextFileID int64

	// failBeforeSwap 是测试钩子：合并文件写出成功后、换 manifest 之前强制失败，
	// 用于验证“合并中途失败可回到合并前一致状态”。
	failBeforeSwap bool
}

// Open 打开（或首次创建）目录 dir 下名为 table 的本地表。
func Open(dir, table string) (*Table, error) {
	const op = "store.open"
	if dir == "" || table == "" {
		return nil, failf(op, ErrInvalidArgument, "dir 和 table 均不能为空")
	}
	ms := newManifestStore(dir, table)
	m, err := ms.Load()
	if err != nil {
		return nil, err
	}
	t := &Table{dir: dir, ms: ms}
	t.cond.L = &t.mmu
	if m == nil {
		// 新表：以空初始列建立版本 1（之后可 AddColumn）。
		hs, err := NewSchemas(table, nil)
		if err != nil {
			return nil, err
		}
		m = buildManifest(1, table, nil, hs, time.Now())
		if err := ms.Save(m); err != nil {
			return nil, err
		}
	}
	hs, err := m.schemaHistory()
	if err != nil {
		return nil, err
	}
	t.cur = m
	t.hs = hs
	t.nextFileID = 0
	for _, f := range m.Files {
		if f.ID > t.nextFileID {
			t.nextFileID = f.ID
		}
	}
	return t, nil
}

func (t *Table) Close() error {
	t.mmu.Lock()
	defer t.mmu.Unlock()
	t.closed = true
	return nil
}

func (t *Table) Schema() *Schema {
	t.mmu.Lock()
	defer t.mmu.Unlock()
	return t.hs.Current()
}

// snapshotLocked 返回当前 manifest（调用方持 mmu）。
func (t *Table) snapshotLocked() *Manifest { return t.cur }

// refSnapshot 注册一次在途读取，返回所见 manifest 快照。
func (t *Table) refSnapshot() *Manifest {
	t.mmu.Lock()
	defer t.mmu.Unlock()
	for t.swapInProgress {
		t.cond.Wait()
	}
	if t.closed {
		return nil
	}
	t.w++
	return t.cur
}

func (t *Table) unrefSnapshot() {
	t.mmu.Lock()
	t.w--
	if t.w < 0 {
		t.w = 0
	}
	if t.w == 0 {
		t.cond.Broadcast()
	}
	t.mmu.Unlock()
}

// beginSwap 等待所有在途读取退出并进入换入窗口。
func (t *Table) beginSwap() {
	t.mmu.Lock()
	defer t.mmu.Unlock()
	t.swapInProgress = true
	for t.w != 0 {
		t.cond.Wait()
	}
}

// commitSwap 发布新 manifest 并结束换入窗口（调用方持 mu）。
func (t *Table) commitSwap(m *Manifest) {
	t.mmu.Lock()
	t.cur = m
	t.swapInProgress = false
	t.cond.Broadcast()
	t.mmu.Unlock()
}

// abortSwap 放弃本次换入并放行等待的读取。
func (t *Table) abortSwap() {
	t.mmu.Lock()
	t.swapInProgress = false
	t.cond.Broadcast()
	t.mmu.Unlock()
}

// publishManifest 在换入窗口内持久化并原子发布新 manifest，随后回收旧清单。
// 调用方须持有 t.mu；进入时尚未开始换入窗口。
func (t *Table) publishManifest(m *Manifest) error {
	t.beginSwap()
	if err := t.ms.Save(m); err != nil {
		t.abortSwap()
		return err
	}
	t.commitSwap(m)
	// 换入成功后回收历史 MANIFEST 文件（CURRENT 已指向新文件）。
	_ = t.ms.RemoveOldManifests(m.ID)
	return nil
}

// Append 校验批次与当前 schema 严格一致后，按列写出一个新数据文件并原子
// 发布 manifest。失败时不改变表状态。
func (t *Table) Append(b *Batch) (int64, error) {
	const op = "store.append"
	if err := b.Validate(); err != nil {
		return 0, err
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	cur := t.hs.Current()
	// 严格按当前 schema 对齐：缺列、多列、类型不符全部明确拒绝。
	if len(b.Columns) != len(cur.Columns) {
		return 0, failf(op, ErrColumnNotFound,
			"批次列数 %d 与当前 schema(v%d) 列数 %d 不一致", len(b.Columns), cur.Version, len(cur.Columns))
	}
	cols := make(map[int]*Column, len(cur.Columns))
	for _, def := range cur.Columns {
		c, ok := b.ColumnByName(def.Name)
		if !ok {
			return 0, failf(op, ErrColumnNotFound, "批次缺少 schema 列 %q (id=%d)", def.Name, def.ID)
		}
		if c.Type != def.Type {
			return 0, failf(op, ErrTypeMismatch,
				"列 %q 批次类型 %s 与 schema(v%d) 类型 %s 不一致", def.Name, c.Type, cur.Version, def.Type)
		}
		cc := *c
		cc.Name = def.Name
		cols[def.ID] = &cc
	}

	id := t.nextFileID + 1
	t.nextFileID = id
	path := filepath.Join(t.ms.tableDir(), dataFileName(id))
	if err := WriteDataFile(path, cur.Version, b.Rows(), cols); err != nil {
		return 0, err
	}
	entry := FileEntry{
		ID:            id,
		Path:          path,
		SchemaVersion: cur.Version,
		NumRows:       b.Rows(),
		CreatedAt:     time.Now(),
	}
	newFiles := append(append([]FileEntry(nil), t.cur.Files...), entry)
	m := buildManifest(t.cur.ID+1, t.hs.Table, newFiles, t.hs, time.Now())
	if err := t.publishManifest(m); err != nil {
		return 0, err
	}
	return id, nil
}

func dataFileName(id int64) string {
	return filepath.Join("data", fmt.Sprintf("data-%07d.ccol", id))
}

// ---- 结构演进 ----

func (t *Table) evolve(build func() (*Schema, error)) (*Schema, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	before := t.hs.Current().Version
	s, err := build()
	if err != nil {
		return nil, err
	}
	if s.Version == before {
		// 无实质变化（如把列改成它本来的类型），不产生新 manifest。
		return s, nil
	}
	m := buildManifest(t.cur.ID+1, t.hs.Table, t.cur.Files, t.hs, time.Now())
	if err := t.publishManifest(m); err != nil {
		return nil, err
	}
	return s, nil
}

func (t *Table) AddColumn(name string, typ ValueType) (*Schema, error) {
	return t.evolve(func() (*Schema, error) { return t.hs.AddColumn(name, typ) })
}

func (t *Table) DropColumn(name string) (*Schema, error) {
	return t.evolve(func() (*Schema, error) { return t.hs.DropColumn(name) })
}

func (t *Table) RenameColumn(oldName, newName string) (*Schema, error) {
	return t.evolve(func() (*Schema, error) { return t.hs.RenameColumn(oldName, newName) })
}

func (t *Table) ChangeColumnType(name string, typ ValueType) (*Schema, error) {
	return t.evolve(func() (*Schema, error) { return t.hs.ChangeColumnType(name, typ) })
}

func (t *Table) setFailBeforeSwap(v bool) {
	t.mu.Lock()
	t.failBeforeSwap = v
	t.mu.Unlock()
}

// Reader 基于当前 manifest 的不可变快照创建读取器。
// cols 为 nil/空 表示读取当前 schema 的全部列；过滤列会自动纳入底层读取。
func (t *Table) Reader(cols []string, f *Filter) (*Reader, error) {
	const op = "store.reader"
	snap := t.refSnapshot()
	if snap == nil {
		return nil, failf(op, ErrInvalidArgument, "表已关闭")
	}
	curSchema := t.hs.Current()

	wanted := map[string]bool{}
	if len(cols) > 0 {
		for _, n := range cols {
			if _, ok := curSchema.ColumnByName(n); !ok {
				t.unrefSnapshot()
				return nil, failf(op, ErrColumnNotFound, "投影列 %q 在当前 schema(v%d) 中不存在", n, curSchema.Version)
			}
			if wanted[n] {
				t.unrefSnapshot()
				return nil, failf(op, ErrInvalidArgument, "投影列重复: %q", n)
			}
			wanted[n] = true
		}
	}
	if f != nil {
		for _, p := range f.Predicates {
			if _, ok := curSchema.ColumnByName(p.Column); !ok {
				t.unrefSnapshot()
				return nil, failf(op, ErrColumnNotFound, "过滤列 %q 在当前 schema(v%d) 中不存在", p.Column, curSchema.Version)
			}
			// 比较类谓词在创建期就核对字面量类型，尽早给出 ErrTypeMismatch。
			if p.Op != OpIsNull && p.Op != OpNotNull {
				def := curSchema.byName[p.Column]
				if p.Literal.Type != def.Type {
					t.unrefSnapshot()
					return nil, failf(op, ErrTypeMismatch,
						"过滤列 %q 字面量类型 %s 与 schema 类型 %s 不一致", p.Column, p.Literal.Type, def.Type)
				}
				if !p.Literal.Valid {
					t.unrefSnapshot()
					return nil, failf(op, ErrInvalidArgument, "比较类谓词 %q 的字面量不能为 NULL", p.Column)
				}
			}
		}
	}

	// 输出列顺序 = 当前 schema 列顺序中被投影的列。
	outCols := make([]string, 0, len(curSchema.Columns))
	for _, c := range curSchema.Columns {
		if len(wanted) == 0 || wanted[c.Name] {
			outCols = append(outCols, c.Name)
		}
	}

	r := &Reader{
		table:  t,
		snap:   snap,
		cols:   outCols,
		filter: f,
	}
	rows, err := r.materialize()
	if err != nil {
		t.unrefSnapshot()
		return nil, err
	}
	r.rows = rows
	r.idx = 0
	return r, nil
}

// Reader 是一次一致性读取的句柄。
type Reader struct {
	table  *Table
	snap   *Manifest
	cols   []string
	filter *Filter
	rows   []map[string]Value
	idx    int
}

// ResultSet 是一行结果。
type ResultSet struct {
	row map[string]Value
}

func (rs *ResultSet) Row() map[string]Value { return rs.row }

// Next 返回下一行；迭代结束 (nil,false,nil)。
func (r *Reader) Next() (*ResultSet, bool, error) {
	if r.idx >= len(r.rows) {
		return nil, false, nil
	}
	rs := &ResultSet{row: r.rows[r.idx]}
	r.idx++
	return rs, true, nil
}

// Close 释放本次读取持有的快照引用。
func (r *Reader) Close() error {
	if r.table != nil {
		r.table.unrefSnapshot()
		r.table = nil
	}
	return nil
}

// materialize 顺序读取快照中的全部文件，只解码投影列与过滤列，
// 逐行应用下推过滤；返回的每行都包含完整投影列（含演进产生的 NULL）。
func (r *Reader) materialize() ([]map[string]Value, error) {
	curSchema := snapshotCurrentSchema(r.snap)
	need := map[string]bool{}
	for _, n := range r.cols {
		need[n] = true
	}
	if r.filter != nil {
		for _, p := range r.filter.Predicates {
			need[p.Column] = true
		}
	}
	// 需要读取的列 ID（当前 schema 视角）。
	type needCol struct {
		id   int
		name string
		typ  ValueType
	}
	var needList []needCol
	for _, def := range curSchema.Columns {
		if need[def.Name] {
			needList = append(needList, needCol{def.ID, def.Name, def.Type})
		}
	}

	files := append([]FileEntry(nil), r.snap.Files...)
	sort.SliceStable(files, func(i, j int) bool { return files[i].ID < files[j].ID })

	var out []map[string]Value
	for _, fe := range files {
		fileSchema, err := schemaAtVersion(r.snap, fe.SchemaVersion)
		if err != nil {
			return nil, err
		}
		dr, err := OpenDataFile(fe.Path)
		if err != nil {
			return nil, err
		}
		n := dr.NumRows()
		columns := make(map[string]*Column, len(needList))
		for _, nc := range needList {
			if defAtFile, ok := fileSchema.ColumnByID(nc.id); ok {
				if !dr.HasColumn(nc.id) {
					dr.Close()
					return nil, failf("datafile.read", ErrCorruptData,
						"文件 %s 的 schema(v%d) 声明了列 %q(id=%d) 但数据块缺失",
						fe.Path, fe.SchemaVersion, nc.name, nc.id)
				}
				fileType, col, err := dr.ReadColumnByID(nc.id)
				if err != nil {
					dr.Close()
					return nil, err
				}
				// 先用文件写入时的 schema 版本核对块类型：不一致即为磁盘损坏，
				// 绝不能把它当成“类型演进”静默转换掉。
				if fileType != defAtFile.Type {
					dr.Close()
					return nil, failf("datafile.read", ErrCorruptData,
						"文件 %s 列 %q(id=%d) 块类型 %s 与 schema(v%d) 记录的 %s 不一致",
						fe.Path, nc.name, nc.id, fileType, fe.SchemaVersion, defAtFile.Type)
				}
				col.Name = nc.name
				// 再按当前 schema 类型做结构演进的兼容即时转换。
				if fileType != nc.typ {
					conv, err := convertColumn(col, nc.typ)
					if err != nil {
						dr.Close()
						return nil, err
					}
					col = conv
				}
				columns[nc.name] = col
			} else {
				// 该文件写入时此列尚不存在（新增列）-> 旧行读为 NULL。
				columns[nc.name] = nullColumn(nc.name, nc.typ, n)
			}
		}
		dr.Close()

		for row := 0; row < n; row++ {
			m := make(map[string]Value, len(needList))
			for _, nc := range needList {
				m[nc.name] = columns[nc.name].ValueAt(row)
			}
			ok, err := r.filter.MatchRow(m)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			proj := make(map[string]Value, len(r.cols))
			for _, name := range r.cols {
				proj[name] = m[name]
			}
			out = append(out, proj)
		}
	}
	return out, nil
}

func snapshotCurrentSchema(m *Manifest) *Schema {
	return m.resolved[len(m.resolved)-1]
}

func schemaAtVersion(m *Manifest, v int) (*Schema, error) {
	if v < 1 || v > len(m.resolved) {
		return nil, failf("manifest.schema", ErrCorruptData, "schema 版本 %d 越界（共 %d 版）", v, len(m.resolved))
	}
	return m.resolved[v-1], nil
}

func nullColumn(name string, t ValueType, rows int) *Column {
	c := &Column{Name: name, Type: t, Nulls: make([]bool, rows)}
	for i := range c.Nulls {
		c.Nulls[i] = true
	}
	switch t {
	case TypeInt64:
		c.I64s = make([]int64, rows)
	case TypeFloat64:
		c.F64s = make([]float64, rows)
	case TypeBool:
		c.Bools = make([]bool, rows)
	case TypeString:
		c.Strs = make([]string, rows)
	}
	return c
}

func convertColumn(c *Column, to ValueType) (*Column, error) {
	out := &Column{Name: c.Name, Type: to, Nulls: append([]bool(nil), c.Nulls...)}
	rows := c.Rows()
	switch to {
	case TypeString:
		out.Strs = make([]string, rows)
	default:
		return nil, failf("schema.convert", ErrTypeMismatch, "列 %q 不支持转换到 %s", c.Name, to)
	}
	for r := 0; r < rows; r++ {
		v, err := ConvertValue(c.ValueAt(r), to)
		if err != nil {
			return nil, err
		}
		if v.Valid {
			out.Strs[r] = v.Str
		}
	}
	return out, nil
}
