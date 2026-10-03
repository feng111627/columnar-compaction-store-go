package columnar

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// buildOneSegmentStore 创建只含一个已落盘段的存储目录，返回 store（已关闭）与段文件路径。
func buildOneSegmentStore(t *testing.T) (dir, segPath string) {
	t.Helper()
	dir = t.TempDir()
	s := mustCreate(t, dir, testSchema2(), Options{FlushThreshold: 100, CompactionMinSegments: 10})
	if err := s.Append(context.Background(), Row{
		"id": int64(1), "score": float64(1.5), "name": "a", "active": true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(context.Background(), Row{
		"id": int64(2), "score": float64(2.5), "name": "bb", "active": false,
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := loadManifest(dir)
	if err != nil {
		t.Fatal(err)
	}
	segPath = filepath.Join(dir, m.Segments[0].FileName)
	return dir, segPath
}

func mutateFile(t *testing.T, path string, fn func(b []byte) []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b = fn(b)
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func reopenAndScan(t *testing.T, dir string, opts ScanOptions) error {
	t.Helper()
	s, err := Open(dir, DefaultOptions())
	if err != nil {
		return err
	}
	defer s.Close()
	sc, err := s.Scan(context.Background(), opts)
	if err != nil {
		return err
	}
	defer sc.Close()
	for {
		_, err := sc.Next()
		if errors.Is(err, errEOF()) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func TestCorruptDataDetection(t *testing.T) {
	biz := "损坏数据-必须报错不静默跳过"

	cases := []struct {
		name    string
		project ScanOptions // 读取选项（验证列裁剪下损坏仍会按列被检测）
		mutate  func(b []byte) []byte
	}{
		{
			name: "魔数被改",
			mutate: func(b []byte) []byte {
				b[0] = 'X'
				return b
			},
		},
		{
			name: "文件头被截断",
			mutate: func(b []byte) []byte {
				return b[:10]
			},
		},
		{
			name: "文件尾部截断(最后一个列帧受损)",
			mutate: func(b []byte) []byte {
				return b[:len(b)-3]
			},
		},
		{
			name: "列值区翻转一个字节(CRC失败)",
			mutate: func(b []byte) []byte {
				b[len(b)-6] ^= 0xFF
				return b
			},
		},
		{
			name:    "只投影name列时损坏active列不影响name",
			project: ScanOptions{Columns: []string{"id", "name"}},
			mutate: func(b []byte) []byte {
				// 翻转靠近文件尾部 active 帧的字节；该列不在投影中，不应被读取
				b[len(b)-2] ^= 0xFF
				return b
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir, segPath := buildOneSegmentStore(t)
			mutateFile(t, segPath, c.mutate)

			opts := c.project
			err := reopenAndScan(t, dir, opts)
			if c.name == "只投影name列时损坏active列不影响name" {
				// 未读取的列即使损坏也必须不影响用到的列（列式裁剪语义）。
				logCase(t, biz, c.name, "corrupt active, project id/name", err)
				if err != nil {
					t.Fatalf("uncorrupted projected columns must still read, got %v", err)
				}
				return
			}
			logCase(t, biz, c.name, "full scan", err)
			if !errors.Is(err, ErrCorruptData) {
				t.Fatalf("%s: want ErrCorruptData, got %v", c.name, err)
			}
		})
	}
}

func TestCorruptProjectedColumnStillDetected(t *testing.T) {
	biz := "损坏数据-投影列损坏必须报错"
	dir, segPath := buildOneSegmentStore(t)
	mutateFile(t, segPath, func(b []byte) []byte {
		b[len(b)-2] ^= 0xFF // active 帧（已由帧偏移检查确认）
		return b
	})
	err := reopenAndScan(t, dir, ScanOptions{Columns: []string{"id", "active"}})
	logCase(t, biz, "投影包含损坏的active列", "project id,active", err)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("want ErrCorruptData when projecting corrupt column, got %v", err)
	}
}

func TestCorruptDataBoolByte(t *testing.T) {
	biz := "损坏数据-非法编码字节"
	dir, segPath := buildOneSegmentStore(t)
	b, _ := os.ReadFile(segPath)
	// active 是最后一列（bool），其值区两个字节 0x01/0x00 紧邻文件尾 CRC 之前。
	// 把第二个 bool 值字节 0x00 改成 0x02（位于末尾 CRC 的前 2 个字节处）。
	idx := len(b) - 5 // 最后 4 字节是 CRC，再往前 1 字节是第二个 bool 值
	t.Logf("[业务=%s] 篡改偏移=%d 原值=%#x", biz, idx, b[idx])
	b[idx] = 0x02
	if err := os.WriteFile(segPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	err := reopenAndScan(t, dir, ScanOptions{})
	logCase(t, biz, "bool值字节=0x02", "full scan", err)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("want ErrCorruptData for illegal bool byte, got %v", err)
	}
}

func TestCorruptManifest(t *testing.T) {
	biz := "损坏数据-清单损坏"
	dir, _ := buildOneSegmentStore(t)

	cases := []struct {
		name string
		fn   func()
		want error
	}{
		{
			name: "manifest不是合法JSON",
			fn: func() {
				_ = os.WriteFile(manifestPath(dir), []byte("{not json"), 0o644)
			},
			want: ErrManifestCorrupt,
		},
		{
			name: "manifest结构版本缺失",
			fn: func() {
				_ = os.WriteFile(manifestPath(dir), []byte(
					`{"version":2,"schema_history":[],"segments":[],"next_segment_id":1}`), 0o644)
			},
			want: ErrManifestCorrupt,
		},
		{
			name: "manifest引用了不存在的schema版本",
			fn: func() {
				_ = os.WriteFile(manifestPath(dir), []byte(
					`{"version":2,"schema_history":[{"version":1,"columns":[{"Name":"id","Type":1,"Nullable":false}]}],"segments":[{"id":1,"file_name":"seg-0000000001.ccs","row_count":2,"schema_version":99}],"next_segment_id":2}`), 0o644)
			},
			want: ErrManifestCorrupt,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.fn()
			_, err := Open(dir, DefaultOptions())
			logCase(t, biz, c.name, "Open", err)
			if !errors.Is(err, c.want) {
				t.Fatalf("want %v, got %v", c.want, err)
			}
		})
	}
}

func TestMissingSegmentFile(t *testing.T) {
	biz := "损坏数据-段文件丢失"
	dir, segPath := buildOneSegmentStore(t)
	if err := os.Remove(segPath); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, DefaultOptions())
	logCase(t, biz, "清单引用的段被删除", segPath, err)
	if !errors.Is(err, ErrCorruptData) {
		t.Fatalf("want ErrCorruptData, got %v", err)
	}
}

func TestOpenMissingDir(t *testing.T) {
	biz := "损坏数据-未初始化目录"
	_, err := Open(filepath.Join(t.TempDir(), "nope"), DefaultOptions())
	logCase(t, biz, "打开不存在的存储", "no manifest", err)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestIncompatibleStoredType(t *testing.T) {
	biz := "损坏数据-存储类型无法演进"
	dir, segPath := buildOneSegmentStore(t)

	// 用"替换清单"的方式模拟：把列类型在清单里改成与存储不兼容的类型
	// （真实的 AlterColumnType 会拒绝这种变更，这里直接构造磁盘上的不自洽状态）。
	mPath := manifestPath(dir)
	raw := `{"version":3,"schema_history":[` +
		`{"version":1,"columns":[` +
		`{"Name":"id","Type":1,"Nullable":false},` +
		`{"Name":"score","Type":2,"Nullable":true},` +
		`{"Name":"name","Type":3,"Nullable":true},` +
		`{"Name":"active","Type":4,"Nullable":true}]},` +
		`{"version":2,"columns":[` +
		`{"Name":"id","Type":1,"Nullable":false},` +
		`{"Name":"score","Type":2,"Nullable":true},` +
		`{"Name":"name","Type":4,"Nullable":true},` +
		`{"Name":"active","Type":4,"Nullable":true}]}],` +
		`"segments":[{"id":1,"file_name":"SEG_PLACEHOLDER","row_count":2,"schema_version":1}],` +
		`"next_segment_id":2}`
	segName := filepath.Base(segPath)
	raw = replaceAll(raw, "SEG_PLACEHOLDER", segName)
	if err := os.WriteFile(mPath, []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}
	err := reopenAndScan(t, dir, ScanOptions{})
	logCase(t, biz, "磁盘上 name 存为 string 但结构声明 bool", "string vs bool", err)
	if !errors.Is(err, ErrIncompatibleSchema) {
		t.Fatalf("want ErrIncompatibleSchema, got %v", err)
	}
}

func replaceAll(s, old, new string) string {
	out := []byte{}
	for i := 0; i < len(s); {
		if i+len(old) <= len(s) && s[i:i+len(old)] == old {
			out = append(out, []byte(new)...)
			i += len(old)
			continue
		}
		out = append(out, s[i])
		i++
	}
	return string(out)
}
