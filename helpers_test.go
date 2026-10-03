package columnar

import (
	"fmt"
	"testing"
)

// testingWriter 把“业务场景 / 输入 / 结论”写入 t.Log，使 go test -v 日志
// 能清晰看出每个用例覆盖了什么。
type testingWriter struct{ t *testing.T }

func (tw *testingWriter) logf(format string, args ...any) {
	tw.t.Helper()
	tw.t.Logf(format, args...)
}

func vI64(v int64) Value      { return Value{Type: TypeInt64, I64: v, Valid: true} }
func vF64(v float64) Value    { return Value{Type: TypeFloat64, F64: v, Valid: true} }
func vStr(v string) Value     { return Value{Type: TypeString, Str: v, Valid: true} }
func vBool(v bool) Value      { return Value{Type: TypeBool, Bool: v, Valid: true} }
func vNull(t ValueType) Value { return Value{Type: t} }

// colI64 等构造器生成稠密列（NULL 位填零值）。
func colI64(name string, vals []int64, nulls []bool) *Column {
	if nulls == nil {
		nulls = make([]bool, len(vals))
	}
	return &Column{Name: name, Type: TypeInt64, I64s: append([]int64(nil), vals...), Nulls: nulls}
}
func colF64(name string, vals []float64, nulls []bool) *Column {
	if nulls == nil {
		nulls = make([]bool, len(vals))
	}
	return &Column{Name: name, Type: TypeFloat64, F64s: append([]float64(nil), vals...), Nulls: nulls}
}
func colStr(name string, vals []string, nulls []bool) *Column {
	if nulls == nil {
		nulls = make([]bool, len(vals))
	}
	return &Column{Name: name, Type: TypeString, Strs: append([]string(nil), vals...), Nulls: nulls}
}
func colBool(name string, vals []bool, nulls []bool) *Column {
	if nulls == nil {
		nulls = make([]bool, len(vals))
	}
	return &Column{Name: name, Type: TypeBool, Bools: append([]bool(nil), vals...), Nulls: nulls}
}

// collectRows 读完整个 reader 并返回行切片。
func collectRows(t *testing.T, r *Reader) []map[string]Value {
	t.Helper()
	var out []map[string]Value
	for {
		rs, ok, err := r.Next()
		if err != nil {
			t.Fatalf("Next 返回错误: %v", err)
		}
		if !ok {
			break
		}
		// 复制一份，避免 ResultSet 复用 map 的潜在别名问题影响断言。
		row := make(map[string]Value, len(rs.Row()))
		for k, v := range rs.Row() {
			row[k] = v
		}
		out = append(out, row)
	}
	return out
}

func fmtVal(v Value) string {
	if !v.Valid {
		return "NULL(" + v.Type.String() + ")"
	}
	switch v.Type {
	case TypeInt64:
		return fmt.Sprintf("%d", v.I64)
	case TypeFloat64:
		return fmt.Sprintf("%v", v.F64)
	case TypeString:
		return v.Str
	case TypeBool:
		return fmt.Sprintf("%t", v.Bool)
	default:
		return "?"
	}
}
