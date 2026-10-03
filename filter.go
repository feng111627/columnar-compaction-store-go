package columnar

// Op 是可下推的简单过滤算子。
type Op int

const (
	OpEq Op = iota + 1 // =
	OpNe               // <>
	OpLt               // <
	OpLe               // <=
	OpGt               // >
	OpGe               // >=
	OpIsNull
	OpNotNull
)

// String 返回算子的稳定文本表示（用于测试日志）。
func (o Op) String() string {
	switch o {
	case OpEq:
		return "="
	case OpNe:
		return "<>"
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpIsNull:
		return "IS NULL"
	case OpNotNull:
		return "IS NOT NULL"
	default:
		return "?op?"
	}
}

// Predicate 是作用在单列上的简单谓词；Filter 为 AND 连接的谓词列表。
// 比较类算子要求 Literal.Type 与列类型一致，否则求值期返回 ErrTypeMismatch
// （绝不把“字符串和数字比”悄悄当成 false）。
type Predicate struct {
	Column  string
	Op      Op
	Literal Value
}

// Filter 为 AND 连接的谓词列表；空 Filter 匹配所有行。
type Filter struct {
	Predicates []Predicate
}

func compareValues(a, b Value) (int, error) {
	const op = "filter.compare"
	if !a.Valid || !b.Valid {
		return 0, failf(op, ErrTypeMismatch, "不能对 NULL 做大小比较")
	}
	if a.Type != b.Type {
		return 0, failf(op, ErrTypeMismatch, "比较两侧类型不一致: %s vs %s", a.Type, b.Type)
	}
	switch a.Type {
	case TypeInt64:
		switch {
		case a.I64 < b.I64:
			return -1, nil
		case a.I64 > b.I64:
			return 1, nil
		default:
			return 0, nil
		}
	case TypeFloat64:
		switch {
		case a.F64 < b.F64:
			return -1, nil
		case a.F64 > b.F64:
			return 1, nil
		default:
			return 0, nil
		}
	case TypeString:
		switch {
		case a.Str < b.Str:
			return -1, nil
		case a.Str > b.Str:
			return 1, nil
		default:
			return 0, nil
		}
	case TypeBool:
		// 布尔只定义相等/不等语义。
		return 0, failf(op, ErrTypeMismatch, "bool 类型不支持大小比较")
	default:
		return 0, failf(op, ErrTypeMismatch, "未知类型 %v", a.Type)
	}
}

// evalOne 求单个谓词。NULL 语义遵循三值逻辑在 AND 下的效果：
// 比较类谓词遇到 NULL 行不满足；Is (Not) Null 直接判定。
func evalOne(p Predicate, row map[string]Value) (bool, error) {
	const op = "filter.eval"
	v, present := row[p.Column]
	if p.Op == OpIsNull {
		// 缺失列对旧行而言等价于“新增列，旧值为 NULL”。
		return !present || !v.Valid, nil
	}
	if p.Op == OpNotNull {
		return present && v.Valid, nil
	}
	if !present {
		// 比较类谓词作用在缺失列上：调用方通常已在投影/校验阶段发现；
		// 这里按不匹配处理而不是报错，避免过滤阶段掩盖真正的结构错误。
		return false, nil
	}
	if !v.Valid {
		return false, nil // NULL 与任何字面量比较都不满足
	}
	if v.Type != p.Literal.Type {
		return false, failf(op, ErrTypeMismatch,
			"谓词列 %q 行值类型 %s 与字面量类型 %s 不匹配", p.Column, v.Type, p.Literal.Type)
	}
	switch p.Op {
	case OpEq, OpNe:
		eq, err := scalarEqual(v, p.Literal)
		if err != nil {
			return false, err
		}
		if p.Op == OpNe {
			return !eq, nil
		}
		return eq, nil
	case OpLt, OpLe, OpGt, OpGe:
		c, err := compareValues(v, p.Literal)
		if err != nil {
			return false, err
		}
		switch p.Op {
		case OpLt:
			return c < 0, nil
		case OpLe:
			return c <= 0, nil
		case OpGt:
			return c > 0, nil
		default:
			return c >= 0, nil
		}
	default:
		return false, failf(op, ErrInvalidArgument, "列 %q 使用了未知算子 %d", p.Column, p.Op)
	}
}

func scalarEqual(a, b Value) (bool, error) {
	c, err := compareValues(a, b)
	if err != nil {
		// bool 走专门的相等判定
		if a.Type == TypeBool && b.Type == TypeBool {
			return a.Bool == b.Bool, nil
		}
		return false, err
	}
	return c == 0, nil
}

// MatchRow 对一行（列名 -> 值）求 AND 连接的过滤结果。
func (f *Filter) MatchRow(row map[string]Value) (bool, error) {
	if f == nil || len(f.Predicates) == 0 {
		return true, nil
	}
	for _, p := range f.Predicates {
		ok, err := evalOne(p, row)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}
