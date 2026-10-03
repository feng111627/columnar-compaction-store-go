package columnar

import (
	"strconv"
)

// ColumnDef 描述一列的当前定义。ID 一旦分配永不复用；删除列只是从新版本摘除，
// 旧文件里该 ID 的数据块仍可被旧版本解读。
type ColumnDef struct {
	ID   int       `json:"id"`
	Name string    `json:"name"`
	Type ValueType `json:"type"`
}

// Schema 描述某个版本的完整表结构。列顺序在版本间保持稳定（新增列追加到尾部）。
type Schema struct {
	Table   string       `json:"table"`
	Version int          `json:"version"`
	Columns []*ColumnDef `json:"columns"`

	byID   map[int]*ColumnDef    `json:"-"`
	byName map[string]*ColumnDef `json:"-"`
}

// NewSchema 构造一个 schema 版本并建立索引；列名/ID 重复或类型非法会被拒绝。
func NewSchema(table string, version int, cols []ColumnDef) (*Schema, error) {
	const op = "schema.new"
	s := &Schema{
		Table:   table,
		Version: version,
		Columns: make([]*ColumnDef, 0, len(cols)),
		byID:    make(map[int]*ColumnDef, len(cols)),
		byName:  make(map[string]*ColumnDef, len(cols)),
	}
	for i := range cols {
		in := cols[i] // 仅读入参；下面 cp 是独立堆对象，Schema 不持有入参切片槽位
		if in.Name == "" {
			return nil, failf(op, ErrInvalidArgument, "版本 %d 第 %d 列名为空", version, i)
		}
		if in.Type == TypeInvalid {
			return nil, failf(op, ErrInvalidArgument, "列 %q 类型非法", in.Name)
		}
		if _, dup := s.byName[in.Name]; dup {
			return nil, failf(op, ErrInvalidArgument, "列名重复: %q", in.Name)
		}
		if _, dup := s.byID[in.ID]; dup {
			return nil, failf(op, ErrInvalidArgument, "列 ID 重复: %d", in.ID)
		}
		cp := in
		s.Columns = append(s.Columns, &cp)
		s.byID[cp.ID] = &cp
		s.byName[cp.Name] = &cp
	}
	return s, nil
}

// ColumnByName 按列名查找。
func (s *Schema) ColumnByName(name string) (*ColumnDef, bool) {
	c, ok := s.byName[name]
	return c, ok
}

// ColumnByID 按列 ID 查找。
func (s *Schema) ColumnByID(id int) (*ColumnDef, bool) {
	c, ok := s.byID[id]
	return c, ok
}

// ColumnIDs 按列顺序返回当前版本所有列 ID。
func (s *Schema) ColumnIDs() []int {
	ids := make([]int, 0, len(s.Columns))
	for _, c := range s.Columns {
		ids = append(ids, c.ID)
	}
	return ids
}

// Schemas 保存一张表的全部 schema 历史；版本号从 1 开始单调递增。
type Schemas struct {
	Table     string    `json:"table"`
	versions  []*Schema `json:"-"`
	NextColID int       `json:"nextColID"`
}

// NewSchemas 用初始列建立版本 1，列 ID 从 0 开始顺序分配。
func NewSchemas(table string, initial []ColumnDef) (*Schemas, error) {
	const op = "schema.init"
	if table == "" {
		return nil, failf(op, ErrInvalidArgument, "表名为空")
	}
	cols := make([]ColumnDef, len(initial))
	for i, c := range initial {
		if c.Name == "" || c.Type == TypeInvalid {
			return nil, failf(op, ErrInvalidArgument, "初始列定义非法: %+v", c)
		}
		cols[i] = ColumnDef{ID: i, Name: c.Name, Type: c.Type}
	}
	s0, err := NewSchema(table, 1, cols)
	if err != nil {
		return nil, err
	}
	return &Schemas{Table: table, versions: []*Schema{s0}, NextColID: len(cols)}, nil
}

// Current 返回最新 schema 版本。
func (hs *Schemas) Current() *Schema { return hs.versions[len(hs.versions)-1] }

// Version 取指定历史版本。
func (hs *Schemas) Version(v int) (*Schema, error) {
	const op = "schema.version"
	if v < 1 || v > len(hs.versions) {
		return nil, failf(op, ErrColumnNotFound, "schema 版本 %d 不存在（当前共 %d 版）", v, len(hs.versions))
	}
	return hs.versions[v-1], nil
}

// appendVersion 用给定列定义派生下一版本。
func (hs *Schemas) appendVersion(cols []ColumnDef) (*Schema, error) {
	s, err := NewSchema(hs.Table, len(hs.versions)+1, cols)
	if err != nil {
		return nil, err
	}
	hs.versions = append(hs.versions, s)
	return s, nil
}

func cloneColDefs(s *Schema) []ColumnDef {
	// 必须分配全新后备数组：NewSchema 会让 Schema.Columns/map 持有本切片
	// 各槽位的地址。若复用 append 的同一后备数组，后续版本的修改会覆盖
	// 旧版本仍在引用的槽位，造成历史 schema 被就地篡改。
	out := make([]ColumnDef, len(s.Columns), len(s.Columns)+1)
	for i, c := range s.Columns {
		out[i] = *c
	}
	return out
}

// AddColumn 在尾部新增一列，分配新的（永不复用的）ID。
func (hs *Schemas) AddColumn(name string, t ValueType) (*Schema, error) {
	const op = "schema.addColumn"
	if name == "" {
		return nil, failf(op, ErrInvalidArgument, "列名为空")
	}
	if t == TypeInvalid {
		return nil, failf(op, ErrInvalidArgument, "列 %q 类型非法", name)
	}
	cur := hs.Current()
	if _, exists := cur.ColumnByName(name); exists {
		return nil, failf(op, ErrInvalidArgument, "列 %q 已存在", name)
	}
	cols := cloneColDefs(cur)
	newID := hs.NextColID
	cols = append(cols, ColumnDef{ID: newID, Name: name, Type: t})
	s, err := hs.appendVersion(cols)
	if err != nil {
		return nil, err
	}
	hs.NextColID++
	return s, nil
}

// DropColumn 从当前版本摘除列（历史版本与旧文件数据保留，ID 不复用）。
func (hs *Schemas) DropColumn(name string) (*Schema, error) {
	const op = "schema.dropColumn"
	cur := hs.Current()
	if _, ok := cur.ColumnByName(name); !ok {
		return nil, failf(op, ErrColumnNotFound, "列 %q 不存在，无法删除", name)
	}
	cols := make([]ColumnDef, 0, len(cur.Columns)-1)
	for _, c := range cur.Columns {
		if c.Name != name {
			cols = append(cols, *c)
		}
	}
	return hs.appendVersion(cols)
}

// RenameColumn 改列名但保留 ID/类型/位置；新旧数据按 ID 对齐，语义连续。
func (hs *Schemas) RenameColumn(oldName, newName string) (*Schema, error) {
	const op = "schema.renameColumn"
	if newName == "" {
		return nil, failf(op, ErrInvalidArgument, "新列名为空")
	}
	cur := hs.Current()
	c, ok := cur.ColumnByName(oldName)
	if !ok {
		return nil, failf(op, ErrColumnNotFound, "列 %q 不存在，无法重命名", oldName)
	}
	if newName != oldName {
		if _, dup := cur.ColumnByName(newName); dup {
			return nil, failf(op, ErrInvalidArgument, "列名 %q 已被占用", newName)
		}
	}
	cols := cloneColDefs(cur)
	for i := range cols {
		if cols[i].ID == c.ID {
			cols[i].Name = newName
		}
	}
	return hs.appendVersion(cols)
}

// ChangeColumnType 申请将列改为 newType。只有存在“无损静态转换”规则的组合才放行；
// 其余一律以 ErrIncompatibleChange 明确拒绝，避免旧数据被静默错位。
func (hs *Schemas) ChangeColumnType(name string, newType ValueType) (*Schema, error) {
	const op = "schema.changeType"
	cur := hs.Current()
	c, ok := cur.ColumnByName(name)
	if !ok {
		return nil, failf(op, ErrColumnNotFound, "列 %q 不存在，无法改类型", name)
	}
	if newType == TypeInvalid {
		return nil, failf(op, ErrInvalidArgument, "列 %q 的新类型非法", name)
	}
	if c.Type == newType {
		return cur, nil // 无实质变化，不产生新版本
	}
	if !IsCompatibleTypeChange(c.Type, newType) {
		return nil, failf(op, ErrIncompatibleChange,
			"列 %q 不能从 %s 安全转换为 %s：不存在无损转换规则", name, c.Type, newType)
	}
	cols := cloneColDefs(cur)
	for i := range cols {
		if cols[i].ID == c.ID {
			cols[i].Type = newType
		}
	}
	return hs.appendVersion(cols)
}

// IsCompatibleTypeChange 判断 from->to 是否存在安全（无损、确定）的即时转换。
//
// 允许的转换（旧数据在读取时按此规则即时转换，无需重写文件）：
//   - bool   -> string : "true" / "false"
//   - int64  -> string : 十进制文本，一一对应且可逆
//
// 特别说明：int64 -> float64 被拒绝，因为超过 2^53 的整数会丢精度，不属于
// 安全转换；任何 string -> 数值、float64 -> int64 等可能失败或越界的组合也
// 全部拒绝。
func IsCompatibleTypeChange(from, to ValueType) bool {
	if from == to {
		return true
	}
	switch from {
	case TypeBool:
		return to == TypeString
	case TypeInt64:
		return to == TypeString
	default:
		return false
	}
}

// ConvertValue 按兼容规则转换单格值；NULL 透传。无法转换时返回 ErrTypeMismatch。
func ConvertValue(v Value, to ValueType) (Value, error) {
	const op = "schema.convert"
	if !v.Valid {
		return Value{Type: to}, nil
	}
	if v.Type == to {
		return v, nil
	}
	switch v.Type {
	case TypeBool:
		if to == TypeString {
			s := "false"
			if v.Bool {
				s = "true"
			}
			return Value{Type: TypeString, Str: s, Valid: true}, nil
		}
	case TypeInt64:
		if to == TypeString {
			return Value{Type: TypeString, Str: strconv.FormatInt(v.I64, 10), Valid: true}, nil
		}
	}
	return Value{}, failf(op, ErrTypeMismatch, "值类型 %s 无法转换为 %s", v.Type, to)
}
