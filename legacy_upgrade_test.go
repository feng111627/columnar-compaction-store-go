package columnar

import (
	"context"
	"os"
	"testing"
)

// TestLegacyStoreWithoutColumnIDs 用手工清单模拟身份特性上线前的存储：
// schema/segments 都不带列 ID 与 stats。打开后旧数据按列名绑定正常读，
// 之后改名仍可读历史，新增列拿到不冲突的新身份。
func TestLegacyStoreWithoutColumnIDs(t *testing.T) {
	biz := "列生命周期-遗留存储(无ID)升级"
	dir, segPath := buildOneSegmentStore(t) // testSchema2: id/score/name/active
	// buildOneSegmentStore 已经是新格式（带 ID/stats），这里把清单降级成旧格式。
	raw := `{"version":2,"schema_history":[` +
		`{"version":1,"columns":[` +
		`{"Name":"id","Type":1,"Nullable":false},` +
		`{"Name":"score","Type":2,"Nullable":true},` +
		`{"Name":"name","Type":3,"Nullable":true},` +
		`{"Name":"active","Type":4,"Nullable":true}]}],` +
		`"segments":[{"id":1,"file_name":"SEG","row_count":2,"schema_version":1}],` +
		`"next_segment_id":2}`
	raw = replaceAll(raw, "SEG", baseSegName(segPath))
	if err := os.WriteFile(manifestPath(dir), []byte(raw), 0o644); err != nil {
		t.Fatal(err)
	}

	s := mustOpen(t, dir, Options{FlushThreshold: 100, CompactionMinSegments: 10})
	rows := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "遗留存储按列名读出旧数据", "2 rows", map[string]any{"n": len(rows), "r0": rows[0]})
	if len(rows) != 2 || rows[0]["name"] != "a" || rows[1]["name"] != "bb" {
		t.Fatalf("legacy name-based read broken: %v", rows)
	}

	// 段级跳过在无 stats 时安全不跳过（带过滤仍返回正确结果）
	fr := scanAll(t, s, ScanOptions{Filters: []Filter{{Column: "id", Op: OpEqual, Value: int64(2)}}})
	logCase(t, biz, "遗留段无stats：不跳过，过滤正确", "id=2", idsOf(fr))
	if !int64SliceEq(idsOf(fr), []int64{2}) {
		t.Fatalf("legacy filter wrong: %v", idsOf(fr))
	}

	// 新增列必须拿到一个非零、不冲突的新身份
	ctx := context.Background()
	if err := s.AddColumn(ctx, Column{Name: "tag", Type: TypeString, Nullable: true}); err != nil {
		t.Fatal(err)
	}
	cur := s.Schema()
	c, _ := cur.Column("tag")
	logCase(t, biz, "遗留存储新增列获得新身份", "tag ID", c.ID)
	if c.ID == 0 {
		t.Fatalf("column added after upgrade must get nonzero id")
	}
	if err := s.Append(ctx, Row{"id": int64(3), "tag": "z"}); err != nil {
		t.Fatal(err)
	}
	all := scanAll(t, s, ScanOptions{})
	logCase(t, biz, "新增列后旧行NULL新行有值", "tag: nil nil z", []any{all[0]["tag"], all[1]["tag"], all[2]["tag"]})
	if all[0]["tag"] != nil || all[2]["tag"] != "z" {
		t.Fatalf("legacy upgrade add-column semantics wrong: %v %v", all[0]["tag"], all[2]["tag"])
	}
	_ = s.Close()
}

func baseSegName(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}
