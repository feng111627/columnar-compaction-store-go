package columnar

import (
	"errors"
	"testing"
)

// 业务覆盖：下推谓词的确定语义
//   - = / <> / < / <= / > / >= 命中集合
//   - NULL 不参与比较（三值逻辑在 AND 下不命中），IS NULL / IS NOT NULL 单列
//   - 类型不符返回 ErrTypeMismatch，而不是静默 false
//   - 缺失列：比较谓词不命中，IS (NOT) NULL 按“旧行新增列为 NULL”处理
func TestFilterSemantics(t *testing.T) {
	tw := &testingWriter{t}

	row := func(vals map[string]Value) map[string]Value { return vals }

	mk := func(i int64) map[string]Value {
		return row(map[string]Value{"x": vI64(i)})
	}
	nullRow := row(map[string]Value{"x": vNull(TypeInt64)})
	boolRow := row(map[string]Value{"b": vBool(true)})

	type tc struct {
		name string
		f    *Filter
		row  map[string]Value
		want bool
		err  bool
	}
	cases := []tc{
		{"5 = 5", &Filter{[]Predicate{{"x", OpEq, vI64(5)}}}, mk(5), true, false},
		{"6 = 5", &Filter{[]Predicate{{"x", OpEq, vI64(5)}}}, mk(6), false, false},
		{"5 <> 6", &Filter{[]Predicate{{"x", OpNe, vI64(6)}}}, mk(5), true, false},
		{"5 < 6", &Filter{[]Predicate{{"x", OpLt, vI64(6)}}}, mk(5), true, false},
		{"6 < 6", &Filter{[]Predicate{{"x", OpLt, vI64(6)}}}, mk(6), false, false},
		{"6 <= 6", &Filter{[]Predicate{{"x", OpLe, vI64(6)}}}, mk(6), true, false},
		{"7 > 6", &Filter{[]Predicate{{"x", OpGt, vI64(6)}}}, mk(7), true, false},
		{"6 >= 6", &Filter{[]Predicate{{"x", OpGe, vI64(6)}}}, mk(6), true, false},
		{"NULL = 5 不命中", &Filter{[]Predicate{{"x", OpEq, vI64(5)}}}, nullRow, false, false},
		{"NULL < 5 不命中", &Filter{[]Predicate{{"x", OpLt, vI64(5)}}}, nullRow, false, false},
		{"x IS NULL", &Filter{[]Predicate{{"x", OpIsNull, Value{}}}}, nullRow, true, false},
		{"x IS NULL 对非空不命中", &Filter{[]Predicate{{"x", OpIsNull, Value{}}}}, mk(1), false, false},
		{"x IS NOT NULL", &Filter{[]Predicate{{"x", OpNotNull, Value{}}}}, mk(1), true, false},
		{"缺失列 IS NULL 命中（旧行）", &Filter{[]Predicate{{"x", OpIsNull, Value{}}}}, row(map[string]Value{}), true, false},
		{"缺失列 IS NOT NULL 不命中", &Filter{[]Predicate{{"x", OpNotNull, Value{}}}}, row(map[string]Value{}), false, false},
		{"bool = true", &Filter{[]Predicate{{"b", OpEq, vBool(true)}}}, boolRow, true, false},
		{"bool < 真 报错", &Filter{[]Predicate{{"b", OpLt, vBool(true)}}}, boolRow, false, true},
		{"int vs string 字面量类型不符", &Filter{[]Predicate{{"x", OpEq, vStr("5")}}}, mk(5), false, true},
		{"AND 连接: x>0 且 x<3", &Filter{[]Predicate{{"x", OpGt, vI64(0)}, {"x", OpLt, vI64(3)}}}, mk(2), true, false},
		{"AND 连接: x>0 且 x<3 对 5 不命中", &Filter{[]Predicate{{"x", OpGt, vI64(0)}, {"x", OpLt, vI64(3)}}}, mk(5), false, false},
	}
	pass, fail := 0, 0
	for _, c := range cases {
		got, err := c.f.MatchRow(c.row)
		if c.err {
			if err == nil {
				t.Fatalf("用例 %q 期望错误但未返回", c.name)
			}
			if !errors.Is(err, ErrTypeMismatch) {
				t.Fatalf("用例 %q 期望 ErrTypeMismatch, got %v", c.name, err)
			}
			tw.logf("[谓词] 输入: %-28s 结论: 按预期报错 -> %v", c.name, err)
			fail++
			continue
		}
		if err != nil {
			t.Fatalf("用例 %q 意外错误: %v", c.name, err)
		}
		if got != c.want {
			t.Fatalf("用例 %q = %v, want %v", c.name, got, c.want)
		}
		pass++
	}
	tw.logf("[谓词] 结论: %d 个命中/不命中断言通过，%d 个类型错误按 ErrTypeMismatch/ErrTypeMismatch 类语义拒绝",
		pass, fail)

	// 空 filter 匹配全部。
	empty := &Filter{}
	if ok, _ := empty.MatchRow(mk(1)); !ok {
		t.Fatal("空 Filter 应匹配任意行")
	}
	var nilFilter *Filter
	if ok, _ := nilFilter.MatchRow(mk(1)); !ok {
		t.Fatal("nil Filter 应匹配任意行")
	}
}
