package columnar

import (
	"fmt"
	"os"
)

// Example 演示从建表、列式写入、投影+过滤读取到合并的完整本地流程。
func Example() {
	dir, err := os.MkdirTemp("", "columnar-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	t, err := Open(dir, "orders")
	if err != nil {
		panic(err)
	}
	defer t.Close()
	if _, err := t.AddColumn("id", TypeInt64); err != nil {
		panic(err)
	}
	if _, err := t.AddColumn("city", TypeString); err != nil {
		panic(err)
	}

	// 两次小批次写入 -> 两个小文件。
	t.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{1, 2}, nil),
		colStr("city", []string{"BJ", "SH"}, nil),
	}})
	t.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{3, 4}, nil),
		colStr("city", []string{"BJ", "SZ"}, nil),
	}})

	// 只读 id 列，并下推 city="BJ" 过滤。
	r, err := t.Reader([]string{"id"}, &Filter{Predicates: []Predicate{
		{Column: "city", Op: OpEq, Literal: vStr("BJ")},
	}})
	if err != nil {
		panic(err)
	}
	for {
		rs, ok, err := r.Next()
		if err != nil {
			panic(err)
		}
		if !ok {
			break
		}
		fmt.Println("matched id =", rs.Row()["id"].I64)
	}
	r.Close()

	stats, err := t.Compact()
	if err != nil {
		panic(err)
	}
	fmt.Printf("compacted %d files into file #%d, %d rows\n",
		stats.InputFiles, stats.OutputFile, stats.RowsOut)

	// Output:
	// matched id = 1
	// matched id = 3
	// compacted 2 files into file #3, 4 rows
}
