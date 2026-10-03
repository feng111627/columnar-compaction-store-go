package columnar

import (
	"context"
	"errors"
	"testing"
)

func TestEdgeCases(t *testing.T) {
	biz := "边界行为"

	t.Run("空存储扫描立即EOF", func(t *testing.T) {
		s := mustCreate(t, t.TempDir(), testSchema2(), DefaultOptions())
		sc, err := s.Scan(context.Background(), ScanOptions{})
		if err != nil {
			t.Fatal(err)
		}
		_, err = sc.Next()
		logCase(t, biz, "空扫描", "0 rows", err)
		if !errors.Is(err, errEOF()) {
			t.Fatalf("want io.EOF, got %v", err)
		}
		if err := sc.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("关闭后操作报错", func(t *testing.T) {
		s := mustCreate(t, t.TempDir(), testSchema2(), DefaultOptions())
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if err := s.Append(context.Background(), Row{"id": int64(1)}); !errors.Is(err, ErrClosed) {
			t.Fatalf("append after close: %v", err)
		}
		if err := s.Flush(context.Background()); !errors.Is(err, ErrClosed) {
			t.Fatalf("flush after close: %v", err)
		}
		if err := s.Close(); !errors.Is(err, ErrClosed) {
			t.Fatalf("double close: %v", err)
		}
		logCase(t, biz, "关闭后Append/Flush/Close", "closed store", "全部 ErrClosed")
	})

	t.Run("重复建库拒绝", func(t *testing.T) {
		dir := t.TempDir()
		s := mustCreate(t, dir, testSchema2(), DefaultOptions())
		_ = s.Close()
		_, err := Create(dir, testSchema2(), DefaultOptions())
		logCase(t, biz, "在已存在目录建库", "Create twice", err)
		if err == nil {
			t.Fatal("second Create must fail")
		}
	})

	t.Run("空结构建库拒绝", func(t *testing.T) {
		_, err := Create(t.TempDir(), Schema{}, DefaultOptions())
		logCase(t, biz, "零列结构", "empty schema", err)
		if !errors.Is(err, ErrIncompatibleSchema) {
			t.Fatalf("want ErrIncompatibleSchema, got %v", err)
		}
	})

	t.Run("NULL行的过滤比较与全列读取", func(t *testing.T) {
		// id 非空无法给 NULL，单独用一个带可空列 v 的存储
		s2 := mustCreate(t, t.TempDir(), Schema{Columns: []Column{
			{Name: "id", Type: TypeInt64, Nullable: false},
			{Name: "v", Type: TypeInt64, Nullable: true},
		}}, Options{FlushThreshold: 10, CompactionMinSegments: 99})
		ctx := context.Background()
		_ = s2.Append(ctx, Row{"id": int64(1), "v": nil})
		_ = s2.Append(ctx, Row{"id": int64(2), "v": int64(5)})
		rows := scanAll(t, s2, ScanOptions{Filters: []Filter{{Column: "v", Op: OpEqual, Value: int64(5)}}})
		logCase(t, biz, "NULL行不参与相等匹配", "v==5", idsOf(rows))
		if len(rows) != 1 || rows[0]["id"] != int64(2) {
			t.Fatalf("NULL row must not match, got %v", rows)
		}
		rows = scanAll(t, s2, ScanOptions{})
		if rows[0]["v"] != nil {
			t.Fatalf("NULL value must round-trip, got %v", rows[0]["v"])
		}
		logCase(t, biz, "NULL全列读取往返", "nil preserved", rows[0]["v"])
		_ = s2.Close()
	})
}
