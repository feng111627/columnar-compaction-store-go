package columnar

import (
	"context"
	"errors"
	"testing"
)

// 日志辅助：统一打印业务域、用例输入与结论，便于从单测日志看出覆盖面。
func logCase(t *testing.T, business, name string, in, got any) {
	t.Helper()
	t.Logf("[业务=%s] 用例=%s | 输入=%v | 结论=%v", business, name, in, got)
}

func baseSchema() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "score", Type: TypeInt64, Nullable: true},
		{Name: "name", Type: TypeString, Nullable: true},
	}}
}

func TestSchemaEvolutionPure(t *testing.T) {
	biz := "表结构演进-纯函数规则"

	t.Run("新增可空列_版本递增", func(t *testing.T) {
		next, err := addColumn(baseSchema(), Column{Name: "tag", Type: TypeString, Nullable: true}, 100)
		if err != nil {
			t.Fatal(err)
		}
		logCase(t, biz, "addColumn(tag nullable)", "tag/string/nullable", next.Columns)
		if next.Version != 1 || len(next.Columns) != 4 {
			t.Fatalf("version=%d cols=%d", next.Version, len(next.Columns))
		}
	})

	t.Run("新增非空列被拒绝", func(t *testing.T) {
		_, err := addColumn(baseSchema(), Column{Name: "tag", Type: TypeString, Nullable: false}, 100)
		logCase(t, biz, "addColumn(tag NOT nullable)", "tag/string/non-null", err)
		if !errors.Is(err, ErrIncompatibleSchema) {
			t.Fatalf("want ErrIncompatibleSchema, got %v", err)
		}
	})

	t.Run("新增重名列被拒绝", func(t *testing.T) {
		_, err := addColumn(baseSchema(), Column{Name: "id", Type: TypeString, Nullable: true}, 100)
		logCase(t, biz, "addColumn(duplicate id)", "id", err)
		if !errors.Is(err, ErrIncompatibleSchema) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("删除存在的列", func(t *testing.T) {
		next, err := dropColumn(baseSchema(), "score")
		if err != nil {
			t.Fatal(err)
		}
		logCase(t, biz, "dropColumn(score)", "score", colNames(next.Columns))
		if _, ok := next.Column("score"); ok {
			t.Fatal("score should be dropped")
		}
	})

	t.Run("删除不存在的列返回列缺失", func(t *testing.T) {
		_, err := dropColumn(baseSchema(), "nope")
		logCase(t, biz, "dropColumn(nope)", "nope", err)
		if !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("want ErrColumnNotFound, got %v", err)
		}
	})

	t.Run("int64转float64允许", func(t *testing.T) {
		next, err := alterType(baseSchema(), "score", TypeFloat64)
		if err != nil {
			t.Fatal(err)
		}
		c, _ := next.Column("score")
		logCase(t, biz, "alterType(score int64->float64)", "int64->float64", c.Type)
		if c.Type != TypeFloat64 {
			t.Fatalf("got %s", c.Type)
		}
	})

	t.Run("string转int64被拒绝", func(t *testing.T) {
		_, err := alterType(baseSchema(), "name", TypeInt64)
		logCase(t, biz, "alterType(name string->int64)", "string->int64", err)
		if !errors.Is(err, ErrIncompatibleSchema) {
			t.Fatalf("want ErrIncompatibleSchema, got %v", err)
		}
	})

	t.Run("int64转string被拒绝", func(t *testing.T) {
		_, err := alterType(baseSchema(), "id", TypeString)
		logCase(t, biz, "alterType(id int64->string)", "int64->string", err)
		if !errors.Is(err, ErrIncompatibleSchema) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("同类型变更幂等", func(t *testing.T) {
		next, err := alterType(baseSchema(), "id", TypeInt64)
		if err != nil {
			t.Fatal(err)
		}
		logCase(t, biz, "alterType(id int64->int64)", "no-op", next.Version)
		if next.Version != baseSchema().Version {
			t.Fatal("no-op alter must not bump version")
		}
	})

	t.Run("canConvert矩阵", func(t *testing.T) {
		cases := []struct {
			from, to DataType
			want     bool
		}{
			{TypeInt64, TypeFloat64, true},
			{TypeInt64, TypeInt64, true},
			{TypeFloat64, TypeInt64, false},
			{TypeString, TypeInt64, false},
			{TypeBool, TypeString, false},
			{TypeFloat64, TypeBool, false},
		}
		for _, c := range cases {
			got := canConvert(c.from, c.to)
			logCase(t, biz, "canConvert", c.from.String()+"->"+c.to.String(), got)
			if got != c.want {
				t.Fatalf("%s->%s want %v", c.from, c.to, c.want)
			}
		}
	})
}

func TestSchemaEvolutionOnStore(t *testing.T) {
	biz := "表结构演进-存储级语义"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, baseSchema(), Options{FlushThreshold: 10, CompactionMinSegments: 5})

	// 旧数据：只有 id/score/name
	if err := s.Append(ctx, Row{"id": int64(1), "score": int64(10), "name": "a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// 新增列 tag 后再写新数据
	if err := s.AddColumn(ctx, Column{Name: "tag", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Row{"id": int64(2), "score": int64(20), "name": "b", "tag": "x"}); err != nil {
		t.Fatal(err)
	}

	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "新增列后混读", "旧行1 + 新行1", rows)
	if len(rows) != 2 {
		t.Fatalf("want 2 rows, got %d", len(rows))
	}
	if rows[0]["tag"] != nil {
		t.Fatalf("old row tag must be NULL, got %v", rows[0]["tag"])
	}
	if rows[1]["tag"] != "x" {
		t.Fatalf("new row tag want x, got %v", rows[1]["tag"])
	}

	// 删除 name 列后：旧列对外不可见，但物理字节仍在旧段里
	if err := s.DropColumn(ctx, "name"); err != nil {
		t.Fatal(err)
	}
	rows = scanAll(t, s, ScanOptions{})
	logCase(t, biz, "删除列后读取", "drop name", keysOf(rows[0]))
	if _, ok := rows[0]["name"]; ok {
		t.Fatal("dropped column must not appear in rows")
	}
	if rows[0]["id"] != int64(1) {
		t.Fatalf("old row id mismatch: %v", rows[0])
	}

	// score int64 -> float64：旧值 10 读成 10.0
	if err := s.AlterColumnType(ctx, "score", TypeFloat64); err != nil {
		t.Fatal(err)
	}
	rows = scanAll(t, s, ScanOptions{})
	logCase(t, biz, "类型提升后混读", "score int64->float64", []any{rows[0]["score"], rows[1]["score"]})
	if rows[0]["score"] != float64(10) || rows[1]["score"] != float64(20) {
		t.Fatalf("score conversion wrong: %v %v", rows[0]["score"], rows[1]["score"])
	}

	// 不兼容变更仍被拒绝：score 再从 float64 改回 int64
	err := s.AlterColumnType(ctx, "score", TypeInt64)
	logCase(t, biz, "float64回退int64被拒绝", "float64->int64", err)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}

	// 演进后写新值必须按新类型：写 int64 给 float64 列应报类型不匹配
	err = s.Append(ctx, Row{"id": int64(3), "score": int64(30), "tag": "z"})
	logCase(t, biz, "演进后旧类型写入被拒绝", "score=int64 to float64 col", err)
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("want ErrTypeMismatch, got %v", err)
	}
	if err := s.Append(ctx, Row{"id": int64(3), "score": float64(30), "tag": "z"}); err != nil {
		t.Fatal(err)
	}

	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

func colNames(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func keysOf(r Row) []string {
	out := make([]string, 0, len(r))
	for k := range r {
		out = append(out, k)
	}
	return out
}

// TestInt64ToFloatAcrossSegments 验证旧段（物理上还是 int64）在类型演进后
// 从磁盘读出时也按 float64 解码，而新写入的段物理上就是 float64。
func TestInt64ToFloatAcrossSegments(t *testing.T) {
	biz := "表结构演进-跨段类型提升"
	dir := t.TempDir()
	ctx := context.Background()
	s := mustCreate(t, dir, Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "v", Type: TypeInt64, Nullable: true},
	}}, Options{FlushThreshold: 100, CompactionMinSegments: 100})

	if err := s.Append(ctx, Row{"id": int64(1), "v": int64(42)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil { // 旧段：v 物理类型 int64
		t.Fatal(err)
	}
	if err := s.AlterColumnType(ctx, "v", TypeFloat64); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, Row{"id": int64(2), "v": float64(7.5)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(ctx); err != nil { // 新段：v 物理类型 float64
		t.Fatal(err)
	}

	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "旧段int64+新段float64混读", "v: 42.0, 7.5", []any{rows[0]["v"], rows[1]["v"]})
	if rows[0]["v"] != float64(42) || rows[1]["v"] != float64(7.5) {
		t.Fatalf("cross-segment conversion wrong: %v %v", rows[0]["v"], rows[1]["v"])
	}
	// 合并后语义保持（合并读时已经是 float64，写出的合并段也是 float64）
	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	rows = scanAll(t, s, ScanOptions{})
	logCase(t, biz, "合并后再读", "post-compaction", []any{rows[0]["v"], rows[1]["v"]})
	if rows[0]["v"] != float64(42) || rows[1]["v"] != float64(7.5) {
		t.Fatalf("post-compaction values wrong: %v %v", rows[0]["v"], rows[1]["v"])
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
