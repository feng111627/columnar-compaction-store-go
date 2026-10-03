package columnar

import (
	"context"
	"errors"
	"io"
	"testing"
)

func testSchema2() Schema {
	return Schema{Columns: []Column{
		{Name: "id", Type: TypeInt64, Nullable: false},
		{Name: "score", Type: TypeFloat64, Nullable: true},
		{Name: "name", Type: TypeString, Nullable: true},
		{Name: "active", Type: TypeBool, Nullable: true},
	}}
}

func TestWriteValidationErrors(t *testing.T) {
	biz := "列式写入-输入校验"
	s := mustCreate(t, t.TempDir(), testSchema2(), DefaultOptions())
	defer s.Close()
	ctx := context.Background()

	cases := []struct {
		name string
		row  Row
		want error
	}{
		{"int列写string", Row{"id": "1"}, ErrTypeMismatch},
		{"float列写int64", Row{"id": int64(1), "score": int64(9)}, ErrTypeMismatch},
		{"bool列写string", Row{"id": int64(1), "active": "yes"}, ErrTypeMismatch},
		{"非空列NULL", Row{"id": nil}, ErrTypeMismatch},
		{"未声明列", Row{"id": int64(1), "ghost": 1}, ErrColumnNotFound},
		{"整行缺id", Row{"name": "no-id"}, ErrTypeMismatch},
	}
	for _, c := range cases {
		err := s.Append(ctx, c.row)
		logCase(t, biz, c.name, c.row, err)
		if !errors.Is(err, c.want) {
			t.Fatalf("%s: want %v, got %v", c.name, c.want, err)
		}
	}
	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "非法写入后存储仍为空", "6 bad rows", len(rows))
	if len(rows) != 0 {
		t.Fatalf("no valid rows should be persisted, got %d", len(rows))
	}
}

func TestColumnarReadProjectionAndFilters(t *testing.T) {
	biz := "列式读取-裁剪与过滤下推"
	s := mustCreate(t, t.TempDir(), testSchema2(), Options{FlushThreshold: 3, CompactionMinSegments: 100})
	defer s.Close()
	ctx := context.Background()
	input := []Row{
		{"id": int64(1), "score": 1.5, "name": "a", "active": true},
		{"id": int64(2), "score": 9.0, "name": "b", "active": false},
		{"id": int64(3), "score": 9.0, "name": "c", "active": true},
		{"id": int64(4), "score": nil, "name": "d", "active": true},
		{"id": int64(5), "score": 7.2, "name": "e", "active": nil},
	}
	for _, r := range input {
		if err := s.Append(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("全列读取顺序与内容", func(t *testing.T) {
		rows := scanAll(t, s, ScanOptions{})
		logCase(t, biz, "scan *", "5 rows (2 segments + mem)", idsOf(rows))
		if len(rows) != 5 || idsOf(rows)[0] != 1 || idsOf(rows)[4] != 5 {
			t.Fatalf("unexpected rows: %v", idsOf(rows))
		}
		if rows[3]["score"] != nil {
			t.Fatalf("NULL must be preserved, got %v", rows[3]["score"])
		}
	})

	t.Run("只加载id和name两列", func(t *testing.T) {
		rows := scanAll(t, s, ScanOptions{Columns: []string{"id", "name"}})
		for _, r := range rows {
			if len(r) != 2 {
				t.Fatalf("projected row must have exactly 2 keys, got %v", r)
			}
			if _, extra := r["score"]; extra {
				t.Fatalf("projected row must not contain score, got %v", r)
			}
		}
		logCase(t, biz, "projection(id,name)", "5 rows", []any{rows[0], rows[4]})
	})

	t.Run("过滤操作符矩阵", func(t *testing.T) {
		cases := []struct {
			name string
			f    Filter
			want []int64
		}{
			{"score==9.0", Filter{"score", OpEqual, float64(9)}, []int64{2, 3}},
			{"id!=3", Filter{"id", OpNotEqual, int64(3)}, []int64{1, 2, 4, 5}},
			{"id<3", Filter{"id", OpLess, int64(3)}, []int64{1, 2}},
			{"id<=3", Filter{"id", OpLessEqual, int64(3)}, []int64{1, 2, 3}},
			{"id>3", Filter{"id", OpGreater, int64(3)}, []int64{4, 5}},
			{"id>=3", Filter{"id", OpGreaterEqual, int64(3)}, []int64{3, 4, 5}},
			{"name==b", Filter{"name", OpEqual, "b"}, []int64{2}},
			{"active==true", Filter{"active", OpEqual, true}, []int64{1, 3, 4}},
			{"score==1.5(NULL不匹配)", Filter{"score", OpEqual, float64(1.5)}, []int64{1}},
		}
		for _, c := range cases {
			rows := scanAll(t, s, ScanOptions{Columns: []string{"id"}, Filters: []Filter{c.f}})
			got := idsOf(rows)
			logCase(t, biz, c.name, c.f, got)
			if !int64SliceEq(got, c.want) {
				t.Fatalf("%s: want %v, got %v", c.name, c.want, got)
			}
		}
	})

	t.Run("过滤与投影组合", func(t *testing.T) {
		rows := scanAll(t, s, ScanOptions{
			Columns: []string{"name"},
			Filters: []Filter{{Column: "id", Op: OpGreater, Value: int64(3)}},
		})
		logCase(t, biz, "project name where id>3", "id>3", []any{rows[0], rows[1]})
		if len(rows) != 2 || rows[0]["name"] != "d" || rows[1]["name"] != "e" {
			t.Fatalf("unexpected %v", rows)
		}
		if _, present := rows[0]["id"]; present {
			t.Fatalf("id must not be loaded into projected row")
		}
	})

	t.Run("读取不存在的列直接报错", func(t *testing.T) {
		_, err := s.Scan(context.Background(), ScanOptions{Columns: []string{"ghost"}})
		logCase(t, biz, "scan unknown column", "ghost", err)
		if !errors.Is(err, ErrColumnNotFound) {
			t.Fatalf("want ErrColumnNotFound, got %v", err)
		}
	})

	t.Run("过滤值类型不符报错", func(t *testing.T) {
		_, err := s.Scan(context.Background(), ScanOptions{Filters: []Filter{{Column: "id", Op: OpEqual, Value: "1"}}})
		logCase(t, biz, "filter type mismatch", "id == \"1\"", err)
		if !errors.Is(err, ErrTypeMismatch) {
			t.Fatalf("want ErrTypeMismatch, got %v", err)
		}
	})

	t.Run("扫描结束返回io.EOF", func(t *testing.T) {
		sc, err := s.Scan(context.Background(), ScanOptions{Columns: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		for {
			_, err := sc.Next()
			if errors.Is(err, io.EOF) {
				logCase(t, biz, "EOF sentinel", "drain", "io.EOF")
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		sc.Close()
	})
}

func int64SliceEq(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
