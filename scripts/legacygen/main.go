// legacygen 用"升级前那一版"（fc50cac，段格式 v1、清单无列生命周期 ID）
// 的代码生成一个真实的老库存储目录，作为兼容性测试的输入。
//
// 本文件不参与主模块构建（scripts/legacygen 是独立 module）。
// 用法见 scripts/gen_legacy_store.sh：脚本把它拷进旧版 worktree 里运行，
// 因此这里只能使用旧版 API（Column 无 ID 字段、无 RenameColumn）。
//
// 生成的老库覆盖以下历史轨迹（全部确定性数据）：
//  1. 建表 id int64 / name string / score float64 / active bool，写 25 行；
//  2. AddColumn(tag string)：再写 10 行（前 25 行 tag 应为 NULL）；
//  3. AlterColumnType(id, float64)：再写 10 行（旧段 id 是 int64 字节）；
//  4. DropColumn(tag) + 同名再加 tag string：再写 10 行（同名重生）；
//  5. Close 落盘。共 55 行、6 个段文件、5 个结构版本。
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	columnar "github.com/feng111627/columnar-compaction-store-go"
)

const totalRows = 55

func rowOf(i int, withTag bool) columnar.Row {
	r := columnar.Row{
		"name":   fmt.Sprintf("name-%03d", i),
		"score":  float64(i) * 1.5,
		"active": i%2 == 0,
	}
	if withTag {
		r["tag"] = fmt.Sprintf("tag-%d", i%5)
	}
	return r
}

func main() {
	out := flag.String("out", "", "输出目录（必须不存在或为空）")
	flag.Parse()
	if *out == "" {
		log.Fatal("missing -out")
	}
	ctx := context.Background()
	opt := columnar.Options{FlushThreshold: 10, CompactionMinSegments: 100}

	schema := columnar.Schema{Columns: []columnar.Column{
		{Name: "id", Type: columnar.TypeInt64},
		{Name: "name", Type: columnar.TypeString},
		{Name: "score", Type: columnar.TypeFloat64},
		{Name: "active", Type: columnar.TypeBool},
	}}
	st, err := columnar.Create(*out, schema, opt)
	must(err, "Create")

	put := func(i int, idFloat bool, withTag bool) {
		r := rowOf(i, withTag)
		if idFloat {
			r["id"] = float64(i)
		} else {
			r["id"] = int64(i)
		}
		must(st.Append(ctx, r), "Append")
	}

	for i := 0; i < 25; i++ { // 段 1-2 + 缓冲，AddColumn 前落盘
		put(i, false, false)
	}
	must(st.AddColumn(ctx, columnar.Column{Name: "tag", Type: columnar.TypeString, Nullable: true}), "AddColumn(tag)")
	for i := 25; i < 35; i++ {
		put(i, false, true)
	}
	must(st.AlterColumnType(ctx, "id", columnar.TypeFloat64), "AlterColumnType(id->float64)")
	for i := 35; i < 45; i++ {
		put(i, true, true)
	}
	must(st.DropColumn(ctx, "tag"), "DropColumn(tag)")
	must(st.AddColumn(ctx, columnar.Column{Name: "tag", Type: columnar.TypeString, Nullable: true}), "AddColumn(tag) 同名重生")
	for i := 45; i < totalRows; i++ {
		put(i, true, true)
	}
	must(st.Close(), "Close")

	// 用旧版代码自己读回一遍，确认生成的老库在旧版下就是完好可读的。
	ro, err := columnar.Open(*out, opt)
	must(err, "reopen")
	sc, err := ro.Scan(ctx, columnar.ScanOptions{})
	must(err, "Scan")
	n := 0
	var idSum float64
	for {
		row, err := sc.Next()
		if err != nil {
			break
		}
		n++
		switch v := row["id"].(type) {
		case int64:
			idSum += float64(v)
		case float64:
			idSum += v
		}
	}
	_ = sc.Close()
	must(ro.Close(), "close")
	if n != totalRows {
		log.Fatalf("legacy readback rows = %d, want %d", n, totalRows)
	}
	fmt.Printf("legacy store generated at %s: rows=%d segments=%d idSum=%.1f\n",
		*out, n, ro.SegmentCount(), idSum)
	// 段数在 Close 后取不到（已关），重新打开统计。
	ro2, err := columnar.Open(*out, opt)
	must(err, "reopen2")
	fmt.Printf("segments on disk: %d\n", ro2.SegmentCount())
	_ = ro2.Close()
	os.Exit(0)
}

func must(err error, what string) {
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
}
