package columnar

// DataType 列的逻辑数据类型。
type DataType int

const (
	TypeInvalid DataType = iota
	TypeInt64
	TypeFloat64
	TypeString
	TypeBool
)

// String 返回类型的可读名称。
func (t DataType) String() string {
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

// Column 列定义。
type Column struct {
	// ID 列的生命周期身份：创建时分配，改名不变；删除后同名重建会分配新 ID。
	// 0 表示未分配（仅出现在手工构造的清单里，此时读取退回按名称匹配）。
	ID       int
	Name     string
	Nullable bool
	Type     DataType
}

// columnByID 按生命周期 ID 查找列，找不到时 ok=false。
func (s *Schema) columnByID(id int) (Column, bool) {
	for i := range s.Columns {
		if s.Columns[i].ID == id {
			return s.Columns[i], true
		}
	}
	return Column{}, false
}

// Schema 表结构快照，带单调递增的版本号。
type Schema struct {
	Version int
	Columns []Column
}

// Column 按名称查找列定义，找不到时 ok=false。
func (s *Schema) Column(name string) (Column, bool) {
	for i := range s.Columns {
		if s.Columns[i].Name == name {
			return s.Columns[i], true
		}
	}
	return Column{}, false
}

// Row 一行数据，nil 表示该列为 NULL。
// int64/float64/string/bool 分别对应四种列类型。
type Row map[string]any

// FilterOp 简单过滤操作符（下推到读取层）。
type FilterOp int

const (
	OpEqual FilterOp = iota
	OpNotEqual
	OpLess
	OpLessEqual
	OpGreater
	OpGreaterEqual
)

func (op FilterOp) String() string {
	switch op {
	case OpEqual:
		return "=="
	case OpNotEqual:
		return "!="
	case OpLess:
		return "<"
	case OpLessEqual:
		return "<="
	case OpGreater:
		return ">"
	case OpGreaterEqual:
		return ">="
	default:
		return "?op"
	}
}

// Filter 作用于单列的过滤条件。
type Filter struct {
	Column string
	Op     FilterOp
	Value  any
}

// ScanOptions 读选项：列裁剪 + 过滤下推。
type ScanOptions struct {
	Columns []string // 空表示读取全部列
	Filters []Filter
}

// Scanner 快照扫描器。Close 释放快照引用，合并操作会等待所有扫描器关闭后
// 才删除旧段文件，因此调用方应尽早 Close。
type Scanner interface {
	Next() (Row, error)
	Close() error
}
