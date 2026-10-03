package columnar

import (
	"errors"
	"testing"
)

// 业务覆盖：表结构演进
//   - 新增列：旧行读为 NULL，新行有值
//   - 删除列：从当前 schema 摘除，旧文件保持不变且历史版本仍可读
//   - 重命名：按列 ID 对齐，旧数据在新名下读出
//   - 兼容类型变化：bool/int64 -> string，旧值即时无损转换
//   - 不兼容类型变化：int64->float64（丢精度）、string->int64 等必须被拒绝，
//     且拒绝原因可被 ErrIncompatibleChange 断言
func TestSchemaEvolution(t *testing.T) {
	tw := &testingWriter{t}
	dir := t.TempDir()
	tbl, err := Open(dir, "events")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer tbl.Close()
	if _, err := tbl.AddColumn("id", TypeInt64); err != nil {
		t.Fatalf("AddColumn id: %v", err)
	}
	if _, err := tbl.AddColumn("flag", TypeBool); err != nil {
		t.Fatalf("AddColumn flag: %v", err)
	}

	// v2 之后写入两行旧数据。
	if _, err := tbl.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{10, 20}, nil),
		colBool("flag", []bool{true, false}, nil),
	}}); err != nil {
		t.Fatalf("Append v2: %v", err)
	}
	tw.logf("[场景1 新增列] 输入: 旧数据 id=[10,20], flag=[true,false]；执行 AddColumn(score,int64)")

	// 场景1：新增列。旧行 score 为 NULL；再写入新行。
	if _, err := tbl.AddColumn("score", TypeInt64); err != nil {
		t.Fatalf("AddColumn score: %v", err)
	}
	if _, err := tbl.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{30}, nil),
		colBool("flag", []bool{true}, nil),
		colI64("score", []int64{99}, nil),
	}}); err != nil {
		t.Fatalf("Append v3: %v", err)
	}
	r, err := tbl.Reader([]string{"id", "score"}, nil)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rows := collectRows(t, r)
	r.Close()
	if len(rows) != 3 {
		t.Fatalf("新增列后行数 = %d, want 3", len(rows))
	}
	if rows[0]["score"].Valid || rows[1]["score"].Valid || rows[2]["score"].I64 != 99 {
		t.Fatalf("旧行 score 应为 NULL，新行应为 99；实际: %v %v %v",
			fmtVal(rows[0]["score"]), fmtVal(rows[1]["score"]), fmtVal(rows[2]["score"]))
	}
	tw.logf("[场景1 新增列] 结论: 旧两行 score=%s,%s；新行 score=%s（旧行 NULL 语义稳定）",
		fmtVal(rows[0]["score"]), fmtVal(rows[1]["score"]), fmtVal(rows[2]["score"]))

	// 场景2：兼容类型变化 bool -> string。
	if _, err := tbl.ChangeColumnType("flag", TypeString); err != nil {
		t.Fatalf("ChangeColumnType flag->string: %v", err)
	}
	r2, err := tbl.Reader([]string{"id", "flag"}, nil)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rows2 := collectRows(t, r2)
	r2.Close()
	wantFlag := []string{"true", "false", "true"}
	for i, w := range wantFlag {
		if rows2[i]["flag"].Type != TypeString || rows2[i]["flag"].Str != w {
			t.Fatalf("flag->string 后第 %d 行 = %v, want %q", i, fmtVal(rows2[i]["flag"]), w)
		}
	}
	tw.logf("[场景2 兼容类型变化] 输入: 旧 bool 列 flag；执行 changeType(string)；结论: 旧值即时转换为 %v",
		[]string{rows2[0]["flag"].Str, rows2[1]["flag"].Str, rows2[2]["flag"].Str})

	// 场景3：int64 -> string 也允许，且值一一对应。
	if _, err := tbl.ChangeColumnType("id", TypeString); err != nil {
		t.Fatalf("ChangeColumnType id->string: %v", err)
	}
	r3, err := tbl.Reader([]string{"id"}, nil)
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	rows3 := collectRows(t, r3)
	r3.Close()
	if rows3[2]["id"].Str != "30" {
		t.Fatalf("id->string 后第 3 行 = %v, want \"30\"", fmtVal(rows3[2]["id"]))
	}
	tw.logf("[场景3 兼容类型变化] 结论: int64 30 -> string %q（可逆无损）", rows3[2]["id"].Str)

	// 场景4：不兼容的类型变化必须被明确拒绝。
	cases := []struct {
		name string
		col  string
		to   ValueType
	}{
		{"string->int64 可能解析失败", "id", TypeInt64},
		{"string->float64 可能解析失败", "id", TypeFloat64},
	}
	for _, c := range cases {
		_, err := tbl.ChangeColumnType(c.col, c.to)
		if !errors.Is(err, ErrIncompatibleChange) {
			t.Fatalf("%s: 期望 ErrIncompatibleChange, got %v", c.name, err)
		}
		tw.logf("[场景4 拒绝不兼容变更] 输入: %s；结论: 明确拒绝 -> %v", c.name, err)
	}
	// 再建一张表验证 int64->float64（精度风险）也被拒绝。
	t2, err := Open(t.TempDir(), "t2")
	if err != nil {
		t.Fatal(err)
	}
	defer t2.Close()
	if _, err := t2.AddColumn("n", TypeInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := t2.ChangeColumnType("n", TypeFloat64); !errors.Is(err, ErrIncompatibleChange) {
		t.Fatalf("int64->float64 应被拒绝, got %v", err)
	}
	tw.logf("[场景4 拒绝不兼容变更] 输入: int64->float64；结论: 明确拒绝（大整数可能丢精度）")

	// 场景5：重命名 —— 按列 ID 对齐，旧数据在新名下读出。
	t3, err := Open(t.TempDir(), "t3")
	if err != nil {
		t.Fatal(err)
	}
	defer t3.Close()
	if _, err := t3.AddColumn("old_name", TypeInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := t3.Append(&Batch{Columns: []*Column{colI64("old_name", []int64{7}, nil)}}); err != nil {
		t.Fatal(err)
	}
	if _, err := t3.RenameColumn("old_name", "new_name"); err != nil {
		t.Fatalf("RenameColumn: %v", err)
	}
	r4, err := t3.Reader([]string{"new_name"}, nil)
	if err != nil {
		t.Fatalf("Reader new_name: %v", err)
	}
	rr := collectRows(t, r4)
	r4.Close()
	if len(rr) != 1 || rr[0]["new_name"].I64 != 7 {
		t.Fatalf("重命名后旧数据应在 new_name 下读出, got %v", rr)
	}
	if _, err := t3.Reader([]string{"old_name"}, nil); !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("重命名后旧名应报 ErrColumnNotFound, got %v", err)
	}
	tw.logf("[场景5 重命名] 输入: old_name=7；结论: 重命名后 new_name=7，旧名读取被拒绝(ErrColumnNotFound)")

	// 场景6：删除列。
	if _, err := t3.AddColumn("temp", TypeString); err != nil {
		t.Fatal(err)
	}
	if _, err := t3.DropColumn("temp"); err != nil {
		t.Fatalf("DropColumn: %v", err)
	}
	if _, err := t3.DropColumn("temp"); !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("重复删除应报 ErrColumnNotFound, got %v", err)
	}
	r5, err := t3.Reader(nil, nil)
	if err != nil {
		t.Fatalf("Reader after drop: %v", err)
	}
	rows5 := collectRows(t, r5)
	r5.Close()
	if len(rows5[0]) != 1 {
		t.Fatalf("删除列后每行只应有 1 列, got %d", len(rows5[0]))
	}
	tw.logf("[场景6 删除列] 结论: temp 从当前 schema 摘除，当前读取仅剩 %d 列；重复删除报 ErrColumnNotFound", len(rows5[0]))
}
