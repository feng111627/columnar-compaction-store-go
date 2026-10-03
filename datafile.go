package columnar

import (
	"os"
	"path/filepath"
	"sort"
	"time"
)

// 数据文件磁盘布局（全部小端）：
//
//	偏移 0  : magic "CCSTDATA" (8B)
//	          u32 formatVersion(=1) | u32 schemaVersion | u32 numRows
//	          重复每列块（列 ID 升序）：
//	            u64 columnID | u32 valueType | u32 encLen | encLen 字节编码块
//	footer  : u32 chunkCount
//	          重复 chunkCount 次：u64 id | u32 type | u64 offset | u32 encLen
//	          u64 footerOffset
//	          magic "CCSTDATA" (8B)
//
// footer 让读取器打开文件时无需扫描全文件即可定位列块，实现“只读用到的列”。
const (
	dataMagic        = "CCSTDATA"
	dataFileFormatV1 = uint32(1)
	dataHeaderLen    = 8 + 4 + 4 + 4
	dataTailMinLen   = 8 + 8
)

// FileEntry 是 manifest 中对一个数据文件的描述。
type FileEntry struct {
	ID            int64     `json:"id"`
	Path          string    `json:"path"`
	SchemaVersion int       `json:"schemaVersion"`
	NumRows       int       `json:"numRows"`
	CreatedAt     time.Time `json:"createdAt"`
}

// writeFileAtomic 写到同目录临时文件，Sync 后 rename 为目标路径。
// 任何时刻目标路径要么不存在/是旧文件、要么是完整新文件。
func writeFileAtomic(path string, data []byte) (err error) {
	const op = "datafile.writeAtomic"
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return failc(op, ErrInvalidArgument, err, "创建目录失败: %s", dir)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-*")
	if err != nil {
		return failc(op, ErrCorruptData, err, "创建临时文件失败: %s", path)
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			tmp.Close()
			os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		return failc(op, ErrCorruptData, err, "写临时文件失败: %s", tmpName)
	}
	if err := tmp.Sync(); err != nil {
		return failc(op, ErrCorruptData, err, "fsync 临时文件失败: %s", tmpName)
	}
	if err := tmp.Close(); err != nil {
		return failc(op, ErrCorruptData, err, "关闭临时文件失败: %s", tmpName)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return failc(op, ErrCorruptData, err, "原子替换失败: %s -> %s", tmpName, path)
	}
	cleanup = false
	return nil
}

// WriteDataFile 一次写完整个数据文件。cols 的键为列 ID；文件按列 ID 升序组织。
func WriteDataFile(path string, schemaVersion int, rows int, cols map[int]*Column) error {
	const op = "datafile.write"
	if rows < 0 {
		return failf(op, ErrInvalidArgument, "行数为负: %d", rows)
	}
	ids := make([]int, 0, len(cols))
	for id, c := range cols {
		if c == nil {
			return failf(op, ErrInvalidArgument, "列 ID %d 数据为 nil", id)
		}
		if c.Rows() != rows {
			return failf(op, ErrTypeMismatch, "列 %q 行数 %d 与文件行数 %d 不一致", c.Name, c.Rows(), rows)
		}
		ids = append(ids, id)
	}
	sort.Ints(ids)

	type chunkRec struct {
		id     uint64
		typ    uint32
		offset uint64
		length uint32
		raw    []byte
	}
	chunks := make([]chunkRec, 0, len(ids))

	buf := make([]byte, 0, dataHeaderLen)
	buf = append(buf, dataMagic...)
	hdr := make([]byte, 12)
	putU32(hdr[0:4], dataFileFormatV1)
	putU32(hdr[4:8], uint32(schemaVersion))
	putU32(hdr[8:12], uint32(rows))
	buf = append(buf, hdr...)

	for _, id := range ids {
		col := cols[id]
		raw, err := encodeChunk(col)
		if err != nil {
			return err
		}
		off := uint64(len(buf))
		h := make([]byte, 8+4+4)
		putU64(h[0:8], uint64(id))
		putU32(h[8:12], uint32(col.Type))
		putU32(h[12:16], uint32(len(raw)))
		buf = append(buf, h...)
		buf = append(buf, raw...)
		chunks = append(chunks, chunkRec{id: uint64(id), typ: uint32(col.Type), offset: off, length: uint32(len(raw)), raw: raw})
	}

	footerOffset := uint64(len(buf))
	fb := make([]byte, 0, 4+len(chunks)*(8+4+8+4)+8+8)
	cnt := make([]byte, 4)
	putU32(cnt, uint32(len(chunks)))
	fb = append(fb, cnt...)
	for _, c := range chunks {
		rec := make([]byte, 8+4+8+4)
		putU64(rec[0:8], c.id)
		putU32(rec[8:12], c.typ)
		putU64(rec[12:20], c.offset)
		putU32(rec[20:24], c.length)
		fb = append(fb, rec...)
	}
	tail := make([]byte, 0, 8+8)
	offb := make([]byte, 8)
	putU64(offb, footerOffset)
	tail = append(tail, offb...)
	tail = append(tail, dataMagic...)
	fb = append(fb, tail...)
	buf = append(buf, fb...)

	return writeFileAtomic(path, buf)
}

// DataReader 只读打开单个数据文件，仅在打开时读取文件头与 footer 建立索引。
type DataReader struct {
	path          string
	raw           []byte
	schemaVersion int
	numRows       int
	footer        map[uint64]chunkIndexEntry
}

type chunkIndexEntry struct {
	typ    uint32
	offset uint64
	length uint32
}

func OpenDataFile(path string) (*DataReader, error) {
	const op = "datafile.open"
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, failc(op, ErrCorruptData, err, "读取数据文件失败: %s", path)
	}
	if len(raw) < dataHeaderLen+4+dataTailMinLen {
		return nil, failf(op, ErrCorruptData, "文件 %s 长度 %d 小于最小布局", path, len(raw))
	}
	if string(raw[0:8]) != dataMagic {
		return nil, failf(op, ErrCorruptData, "文件 %s 头部魔数不匹配", path)
	}
	if v := u32(raw[8:12]); v != dataFileFormatV1 {
		return nil, failf(op, ErrCorruptData, "文件 %s 格式版本 %d 不受支持", path, v)
	}
	sv := int(u32(raw[12:16]))
	nr := int(u32(raw[16:20]))

	tail := len(raw) - dataTailMinLen
	if string(raw[len(raw)-8:]) != dataMagic {
		return nil, failf(op, ErrCorruptData, "文件 %s 尾部魔数不匹配", path)
	}
	footerOffset := int(u64(raw[tail : tail+8]))
	if footerOffset < dataHeaderLen || footerOffset > tail {
		return nil, failf(op, ErrCorruptData, "文件 %s footer 偏移 %d 越界", path, footerOffset)
	}
	p := footerOffset
	if p+4 > tail {
		return nil, failf(op, ErrCorruptData, "文件 %s footer 截断（计数）", path)
	}
	chunkCount := int(u32(raw[p : p+4]))
	p += 4
	footer := make(map[uint64]chunkIndexEntry, chunkCount)
	const recLen = 8 + 4 + 8 + 4
	if p+chunkCount*recLen+8+8 != len(raw) {
		return nil, failf(op, ErrCorruptData, "文件 %s footer 记录数 %d 与文件长度不自洽", path, chunkCount)
	}
	for i := 0; i < chunkCount; i++ {
		rec := raw[p : p+recLen]
		id := u64(rec[0:8])
		typ := u32(rec[8:12])
		off := u64(rec[12:20])
		ln := u32(rec[20:24])
		if _, dup := footer[id]; dup {
			return nil, failf(op, ErrCorruptData, "文件 %s footer 列 ID %d 重复", path, id)
		}
		if ValueType(typ) == TypeInvalid || ValueType(typ) > TypeBool {
			return nil, failf(op, ErrCorruptData, "文件 %s 列 ID %d 类型码 %d 非法", path, id, typ)
		}
		if int(off) < dataHeaderLen || int(off)+16+int(ln) > footerOffset {
			return nil, failf(op, ErrCorruptData, "文件 %s 列 ID %d 块范围越界", path, id)
		}
		footer[id] = chunkIndexEntry{typ: typ, offset: off, length: ln}
		p += recLen
	}
	if int(u64(raw[p:p+8])) != footerOffset {
		return nil, failf(op, ErrCorruptData, "文件 %s footer 偏移回指不一致", path)
	}
	return &DataReader{
		path:          path,
		raw:           raw,
		schemaVersion: sv,
		numRows:       nr,
		footer:        footer,
	}, nil
}

func (r *DataReader) Path() string       { return r.path }
func (r *DataReader) SchemaVersion() int { return r.schemaVersion }
func (r *DataReader) NumRows() int       { return r.numRows }
func (r *DataReader) HasColumn(id int) bool {
	_, ok := r.footer[uint64(id)]
	return ok
}

// ReadColumnByID 仅解码请求列的块，返回块在文件中记录的原始类型（供读取期做
// 类型演进转换）。列不存在返回 ErrColumnNotFound，块损坏返回 ErrCorruptData。
func (r *DataReader) ReadColumnByID(id int) (ValueType, *Column, error) {
	const op = "datafile.readColumn"
	e, ok := r.footer[uint64(id)]
	if !ok {
		return TypeInvalid, nil, failf(op, ErrColumnNotFound, "文件 %s 不含列 ID %d", r.path, id)
	}
	// 块头 u64 id | u32 type | u32 len，紧跟编码块；以 footer 偏移交叉校验。
	p := int(e.offset)
	if p+16 > len(r.raw) {
		return TypeInvalid, nil, failf(op, ErrCorruptData, "文件 %s 列 ID %d 块头截断", r.path, id)
	}
	chunkID := u64(r.raw[p : p+8])
	chunkType := u32(r.raw[p+8 : p+12])
	chunkLen := u32(r.raw[p+12 : p+16])
	if chunkID != uint64(id) || chunkType != e.typ || chunkLen != e.length {
		return TypeInvalid, nil, failf(op, ErrCorruptData, "文件 %s 列 ID %d 块头与 footer 不一致", r.path, id)
	}
	enc := r.raw[p+16 : p+16+int(chunkLen)]
	col, err := decodeChunk(enc, r.numRows, "", ValueType(chunkType))
	if err != nil {
		return TypeInvalid, nil, err
	}
	return ValueType(chunkType), col, nil
}

func (r *DataReader) Close() error {
	if r != nil {
		r.raw = nil
	}
	return nil
}
