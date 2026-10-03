package columnar

import "fmt"

// ValueType 是组件支持的标量列类型集合。
type ValueType int

const (
	TypeInvalid ValueType = iota
	TypeInt64
	TypeFloat64
	TypeString
	TypeBool
)

// String 返回类型的稳定名称（也用于落盘清单的可读调试）。
func (t ValueType) String() string {
	switch t {
	case TypeInt64:
		return "int64"
	case TypeFloat64:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	default:
		return "invalid"
	}
}

// Value 是单格值的统一表示，Valid 为 false 表示 NULL。
type Value struct {
	Type ValueType
	I64  int64
	F64  float64
	Str  string
	Bool bool
	// Valid 为 false 时其它字段无意义。
	Valid bool
}

// NullValue 构造一个指定类型的 NULL 值。
func NullValue(t ValueType) Value { return Value{Type: t} }

// Column 是单列的列式数据，Nulls 与各类型切片长度均等于 Rows()。
// 各类型值切片采用“稠密”布局：长度必须等于行数，NULL 位填零值，
// 这样按行索引 O(1)，编码时再按 Nulls 跳过 NULL。
type Column struct {
	Name  string
	Type  ValueType
	I64s  []int64
	F64s  []float64
	Strs  []string
	Bools []bool
	Nulls []bool // true 表示该行为 NULL
}

// Rows 返回列的行数。
func (c *Column) Rows() int {
	if c == nil {
		return 0
	}
	return len(c.Nulls)
}

// Valid 检查列式结构是否自洽：类型已知、各切片等长、类型与值切片匹配。
func (c *Column) Valid() error {
	const op = "batch.validate"
	if c == nil {
		return failf(op, ErrInvalidArgument, "nil column")
	}
	n := len(c.Nulls)
	switch c.Type {
	case TypeInt64:
		if len(c.I64s) != n {
			return failf(op, ErrTypeMismatch, "column %q int64 切片长度 %d 与行数 %d 不一致", c.Name, len(c.I64s), n)
		}
	case TypeFloat64:
		if len(c.F64s) != n {
			return failf(op, ErrTypeMismatch, "column %q float64 切片长度 %d 与行数 %d 不一致", c.Name, len(c.F64s), n)
		}
	case TypeString:
		if len(c.Strs) != n {
			return failf(op, ErrTypeMismatch, "column %q string 切片长度 %d 与行数 %d 不一致", c.Name, len(c.Strs), n)
		}
	case TypeBool:
		if len(c.Bools) != n {
			return failf(op, ErrTypeMismatch, "column %q bool 切片长度 %d 与行数 %d 不一致", c.Name, len(c.Bools), n)
		}
	default:
		return failf(op, ErrTypeMismatch, "column %q 类型未知: %v", c.Name, c.Type)
	}
	return nil
}

// ValueAt 读取第 row 行的值（O(1)，不分配新切片）。
func (c *Column) ValueAt(row int) Value {
	if row < 0 || row >= len(c.Nulls) {
		return Value{}
	}
	if c.Nulls[row] {
		return Value{Type: c.Type}
	}
	switch c.Type {
	case TypeInt64:
		return Value{Type: c.Type, I64: c.I64s[row], Valid: true}
	case TypeFloat64:
		return Value{Type: c.Type, F64: c.F64s[row], Valid: true}
	case TypeString:
		return Value{Type: c.Type, Str: c.Strs[row], Valid: true}
	case TypeBool:
		return Value{Type: c.Type, Bool: c.Bools[row], Valid: true}
	default:
		return Value{}
	}
}

// Batch 是一次写入的多行数据；各列等长。
type Batch struct {
	Columns []*Column
}

// Rows 返回批内行数（无列时为 0）。
func (b *Batch) Rows() int {
	if len(b.Columns) == 0 {
		return 0
	}
	return b.Columns[0].Rows()
}

// ColumnByName 按名找列。
func (b *Batch) ColumnByName(name string) (*Column, bool) {
	for _, c := range b.Columns {
		if c != nil && c.Name == name {
			return c, true
		}
	}
	return nil, false
}

// Validate 校验批次：列结构合法、行数一致、列名不重复；列缺失/类型问题由
// Append 对照 schema 进一步判定（那里能给出 ErrColumnNotFound/ErrTypeMismatch）。
func (b *Batch) Validate() error {
	const op = "batch.validate"
	if b == nil || len(b.Columns) == 0 {
		return failf(op, ErrInvalidArgument, "空批次")
	}
	n := b.Rows()
	seen := make(map[string]struct{}, len(b.Columns))
	for _, c := range b.Columns {
		if err := c.Valid(); err != nil {
			return err
		}
		if c.Rows() != n {
			return failf(op, ErrInvalidArgument, "列 %q 行数 %d 与批次行数 %d 不一致", c.Name, c.Rows(), n)
		}
		if _, dup := seen[c.Name]; dup {
			return failf(op, ErrInvalidArgument, "批次内列名重复: %q", fmt.Sprintf("%s", c.Name))
		}
		seen[c.Name] = struct{}{}
	}
	return nil
}
