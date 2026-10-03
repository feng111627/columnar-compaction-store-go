package columnar

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// 业务覆盖：损坏或不兼容数据必须以可区分的错误原因暴露，绝不静默跳过。
//   - 数据块 CRC 损坏           -> ErrCorruptData
//   - 头部魔数被改             -> ErrCorruptData
//   - 文件截断                 -> ErrCorruptData
//   - 块类型码被改成未知值      -> ErrCorruptData
//   - 批次类型与 schema 不符    -> ErrTypeMismatch
//   - 批次缺列/多列            -> ErrColumnNotFound
//   - 编码层长度切片不一致      -> ErrTypeMismatch
//   - manifest 被损坏          -> ErrCorruptData
func TestCorruptDataDetection(t *testing.T) {
	tw := &testingWriter{t}
	dir := t.TempDir()
	tbl, err := Open(dir, "c")
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	if _, err := tbl.AddColumn("id", TypeInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.AddColumn("name", TypeString); err != nil {
		t.Fatal(err)
	}
	good := &Batch{Columns: []*Column{
		colI64("id", []int64{1, 2, 3}, nil),
		colStr("name", []string{"a", "bb", "ccc"}, nil),
	}}
	if _, err := tbl.Append(good); err != nil {
		t.Fatal(err)
	}

	// 用例1：CRC/payload 损坏。
	func() {
		dir2 := t.TempDir()
		t2, _ := Open(dir2, "c1")
		t2.AddColumn("id", TypeInt64)
		t2.Append(&Batch{Columns: []*Column{colI64("id", []int64{1, 2, 3}, nil)}})
		f := t2.cur.Files[0].Path
		st, _ := os.Stat(f)
		// 打在文件靠后但避开 footer 魔数/偏移回指的位置，命中某个编码字节，
		// 造成 CRC 不匹配。
		writeByteAt(f, st.Size()-40, 0xFF)
		t2.Close()
		t3, err := Open(dir2, "c1")
		if err != nil {
			t.Fatalf("Open 本身不应因单文件损坏失败: %v", err)
		}
		defer t3.Close()
		r, err := t3.Reader(nil, nil)
		if err == nil {
			_, _, e := r.Next()
			if !errors.Is(e, ErrCorruptData) {
				t.Fatalf("期望读取期 ErrCorruptData, got %v", e)
			}
			r.Close()
		} else if !errors.Is(err, ErrCorruptData) {
			t.Fatalf("期望 ErrCorruptData, got %v", err)
		}
		tw.logf("[损坏 CRC] 输入: 翻转编码字节；结论: ErrCorruptData（CRC32C 不匹配）, err=%v", errOrReaderErr(err, r))
	}()

	// 用例2：头部魔数损坏。
	func() {
		dir2 := t.TempDir()
		t2, _ := Open(dir2, "c2")
		t2.AddColumn("id", TypeInt64)
		t2.Append(&Batch{Columns: []*Column{colI64("id", []int64{1}, nil)}})
		f := t2.cur.Files[0].Path
		writeByteAt(f, 0, 'X')
		t2.Close()
		t3, _ := Open(dir2, "c2")
		defer t3.Close()
		_, err := t3.Reader(nil, nil)
		if !errors.Is(err, ErrCorruptData) {
			t.Fatalf("魔数损坏应报 ErrCorruptData, got %v", err)
		}
		tw.logf("[损坏 头魔数] 结论: ErrCorruptData: %v", err)
	}()

	// 用例3：文件截断。
	func() {
		dir2 := t.TempDir()
		t2, _ := Open(dir2, "c3")
		t2.AddColumn("id", TypeInt64)
		t2.Append(&Batch{Columns: []*Column{colI64("id", []int64{1, 2, 3, 4}, nil)}})
		f := t2.cur.Files[0].Path
		st, _ := os.Stat(f)
		data, _ := os.ReadFile(f)
		os.WriteFile(f, data[:st.Size()/2], 0o644)
		t2.Close()
		t3, _ := Open(dir2, "c3")
		defer t3.Close()
		_, err := t3.Reader(nil, nil)
		if !errors.Is(err, ErrCorruptData) {
			t.Fatalf("文件截断应报 ErrCorruptData, got %v", err)
		}
		tw.logf("[损坏 截断] 结论: ErrCorruptData: %v", err)
	}()

	// 用例4：footer 中块类型码被改成非法值（footer 结构仍自洽）。
	func() {
		dir2 := t.TempDir()
		t2, _ := Open(dir2, "c4")
		t2.AddColumn("id", TypeInt64)
		t2.Append(&Batch{Columns: []*Column{colI64("id", []int64{1, 2}, nil)}})
		f := t2.cur.Files[0].Path
		t2.Close()

		data, _ := os.ReadFile(f)
		footerOff := int(binary.LittleEndian.Uint64(data[len(data)-16 : len(data)-8]))
		// footer: u32 count | 每条 24 字节；第 1 条的 type 在偏移 +8+4。
		typeOff := footerOff + 4 + 8
		data[typeOff] = 99 // 非法类型码
		os.WriteFile(f, data, 0o644)

		t3, _ := Open(dir2, "c4")
		defer t3.Close()
		_, err := t3.Reader(nil, nil)
		if !errors.Is(err, ErrCorruptData) {
			t.Fatalf("非法类型码应报 ErrCorruptData, got %v", err)
		}
		tw.logf("[损坏 类型码] 结论: ErrCorruptData: %v", err)
	}()

	// 用例5：写入期不兼容 —— 列类型对不上。
	_, err = tbl.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{1}, nil),
		colBool("name", []bool{true}, nil), // schema 要求 string
	}})
	if !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("写入类型错位应报 ErrTypeMismatch, got %v", err)
	}
	tw.logf("[不兼容 写入类型] 输入: name 给 bool 而 schema 是 string；结论: ErrTypeMismatch: %v", err)

	// 用例6：缺列、多列。
	_, err = tbl.Append(&Batch{Columns: []*Column{colI64("id", []int64{1}, nil)}})
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("缺列应报 ErrColumnNotFound, got %v", err)
	}
	_, err = tbl.Append(&Batch{Columns: []*Column{
		colI64("id", []int64{1}, nil),
		colStr("name", []string{"a"}, nil),
		colStr("extra", []string{"x"}, nil),
	}})
	if !errors.Is(err, ErrColumnNotFound) {
		t.Fatalf("多列应报 ErrColumnNotFound, got %v", err)
	}
	tw.logf("[不兼容 缺列/多列] 结论: 缺 name -> ErrColumnNotFound；多 extra -> ErrColumnNotFound")

	// 用例7：列内部切片长度不自洽。
	badCol := &Column{Name: "id", Type: TypeInt64, I64s: []int64{1}, Nulls: []bool{false, false}}
	if err := badCol.Valid(); !errors.Is(err, ErrTypeMismatch) {
		t.Fatalf("切片长度不一致应报 ErrTypeMismatch, got %v", err)
	}
	tw.logf("[不合法 列结构] 输入: I64s 长度1 / Nulls 长度2；结论: ErrTypeMismatch")

	// 用例8：编码层直接损坏（CRC）。
	raw, encErr := encodeChunk(colI64("id", []int64{1, 2}, nil))
	if encErr != nil {
		t.Fatal(encErr)
	}
	raw[len(raw)-5] ^= 0xFF
	if _, derr := decodeChunk(raw, 2, "id", TypeInt64); !errors.Is(derr, ErrCorruptData) {
		t.Fatalf("编码块 CRC 损坏应报 ErrCorruptData, got %v", derr)
	}
	tw.logf("[损坏 编码CRC] 结论: decodeChunk 返回 ErrCorruptData")

	// 用例9：声明行数与 payload 不符（少字节）。
	raw2, _ := encodeChunk(colI64("id", []int64{1}, nil))
	if _, derr := decodeChunk(raw2, 2, "id", TypeInt64); !errors.Is(derr, ErrCorruptData) {
		t.Fatalf("行数与 payload 不符应报 ErrCorruptData, got %v", derr)
	}
	tw.logf("[损坏 行数不符] 输入: 块只含 1 行却声明 2 行；结论: ErrCorruptData")
}

// 业务覆盖：manifest / CURRENT 损坏必须报 ErrCorruptData，不能退化成空表。
func TestCorruptManifest(t *testing.T) {
	tw := &testingWriter{t}
	dir := t.TempDir()
	tbl, err := Open(dir, "m")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.AddColumn("id", TypeInt64); err != nil {
		t.Fatal(err)
	}
	if _, err := tbl.Append(&Batch{Columns: []*Column{colI64("id", []int64{1}, nil)}}); err != nil {
		t.Fatal(err)
	}
	tbl.Close()

	// 损坏 CURRENT 指向的 manifest JSON。
	curBytes, _ := os.ReadFile(filepath.Join(dir, "tables", "m", "CURRENT"))
	mp := filepath.Join(dir, "tables", "m", string(curBytes[:len(curBytes)-1]))
	st, _ := os.Stat(mp)
	data, _ := os.ReadFile(mp)
	// 破坏 JSON 中间一个字符，保持长度不变。
	mid := st.Size() / 2
	data[mid] = '#'
	os.WriteFile(mp, data, 0o644)

	if _, err := Open(dir, "m"); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("manifest JSON 损坏应报 ErrCorruptData 而非静默新建空表, got %v", err)
	}
	tw.logf("[损坏 manifest] 输入: 翻转 MANIFEST JSON 中间字节；结论: ErrCorruptData（不会退化成空表）")

	// CURRENT 指向不存在的 manifest。
	os.WriteFile(filepath.Join(dir, "tables", "m", "CURRENT"), []byte("MANIFEST-00099999\n"), 0o644)
	if _, err := Open(dir, "m"); !errors.Is(err, ErrCorruptData) {
		t.Fatalf("CURRENT 悬空应报 ErrCorruptData, got %v", err)
	}
	tw.logf("[损坏 CURRENT] 输入: CURRENT 指向不存在的 MANIFEST；结论: ErrCorruptData")
}

var errOrReaderErr = func(err error, r *Reader) error {
	if err != nil {
		return err
	}
	if r == nil {
		return nil
	}
	_, _, e := r.Next()
	return e
}
