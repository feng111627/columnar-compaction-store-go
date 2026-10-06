package columnar

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
)

// 列式数据文件格式（全部整数为大端）：
//
//	固定头(16 字节):
//	  magic[4]="CCS1" | version u16 | schemaVersion u32 | rowCount u32 | nCols u16
//	统计块（version >= 2 才有；段级跳过的依据，与数据同文件、同生命周期）:
//	  statsLen u32 | statsPayload | crc u32   crc 覆盖 statsPayload
//	列目录（nCols 项，使读取方可以不碰未用列的数据帧）:
//	  nameLen u8 | name bytes | type u8 | frameLen u32
//	列帧（顺序与目录一致）:
//	  payload:
//	    nameLen u16 | name bytes     帧内自带身份信息，纳入 CRC
//	    type u8 | count u32          与目录/头交叉校验
//	    nullBitmap  ceil(count/8) 字节，置 1 表示非 NULL
//	    values      按类型紧密排列，仅非 NULL 值
//	  crc u32    crc32-IEEE(payload)
//
// 目录给出每帧长度，读取时按偏移直接读取所需列，实现真正的列裁剪；
// CRC、长度与类型交叉校验保证任何字节损坏/截断都以 ErrCorruptData 暴露。
// 统计块损坏不算数据损坏：读取方静默退回完整扫描（见 stats.go）。
//
// 版本兼容：v1 是升级前老库写出的格式（固定头后直接是列目录，无统计块），
// 读取侧完整支持；v2 是当前写出的格式。超出 [v1, v2] 范围的版本号说明文件
// 来自更新（或无法识别）的实现，报 ErrUnsupportedVersion 而非 ErrCorruptData。

const segmentMagic = "CCS1"

// segmentFormatVersion 当前写出的格式版本。v2 起固定头后多了统计块。
// 读取侧同时支持 v1（升级前的老段：固定头后直接是列目录，无统计块）。
const segmentFormatVersion uint16 = 2

// segmentFormatVersionOldest 读取侧支持的最老格式版本。
const segmentFormatVersionOldest uint16 = 1

var crcTable = crc32.MakeTable(crc32.IEEE)

type decodedColumn struct {
	typ  DataType
	vals []any // 长度等于行数；NULL 位置为 nil
}

type dirEntry struct {
	name     string
	typ      DataType
	frameLen int // payload + crc
	offset   int64
}

type segmentFileHeader struct {
	version       uint16
	schemaVersion int
	rowCount      int
	dir           []dirEntry
	dataStart     int64
}

// encodeSegment 把若干行按 schema 列顺序编码成完整段文件字节。
// 统计块从真实行计算（computeStats），与数据同生同灭。
func encodeSegment(schemaVersion int, rows []Row, schema Schema) ([]byte, error) {
	frames := make([][]byte, len(schema.Columns))
	dir := make([]dirEntry, len(schema.Columns))

	var dataLen int
	for i, col := range schema.Columns {
		frame, err := buildColumnFrame(col, rows)
		if err != nil {
			return nil, err
		}
		frames[i] = frame
		dir[i] = dirEntry{name: col.Name, typ: col.Type, frameLen: len(frame)}
		dataLen += len(frame)
	}

	statsPayload := encodeStats(computeStats(schema, rows), schema.Columns)

	var buf bytes.Buffer
	buf.WriteString(segmentMagic)
	_ = binary.Write(&buf, binary.BigEndian, segmentFormatVersion)
	_ = binary.Write(&buf, binary.BigEndian, uint32(schemaVersion))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(rows)))
	_ = binary.Write(&buf, binary.BigEndian, uint16(len(schema.Columns)))

	_ = binary.Write(&buf, binary.BigEndian, uint32(len(statsPayload)))
	buf.Write(statsPayload)
	_ = binary.Write(&buf, binary.BigEndian, crc32.Checksum(statsPayload, crcTable))

	for _, e := range dir {
		nb := []byte(e.name)
		if len(nb) > 255 {
			return nil, fmt.Errorf("%w: column name too long %q", ErrCorruptData, e.name)
		}
		buf.WriteByte(byte(len(nb)))
		buf.Write(nb)
		buf.WriteByte(byte(e.typ))
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(e.frameLen))
		buf.Write(lb[:])
	}

	offset := int64(buf.Len())
	for i := range frames {
		dir[i].offset = offset
		offset += int64(len(frames[i]))
		buf.Write(frames[i])
	}
	return buf.Bytes(), nil
}

func buildColumnFrame(col Column, rows []Row) ([]byte, error) {
	var payload bytes.Buffer
	nb := []byte(col.Name)
	_ = binary.Write(&payload, binary.BigEndian, uint16(len(nb)))
	payload.Write(nb)
	payload.WriteByte(byte(col.Type))
	_ = binary.Write(&payload, binary.BigEndian, uint32(len(rows)))

	bitmap := make([]byte, (len(rows)+7)/8)
	var values bytes.Buffer
	for i, row := range rows {
		v := row[col.Name]
		if v == nil {
			continue
		}
		bitmap[i/8] |= 1 << uint(i%8)
		if err := encodeValue(&values, col.Name, col.Type, v); err != nil {
			return nil, err
		}
	}
	payload.Write(bitmap)
	payload.Write(values.Bytes())

	var out bytes.Buffer
	out.Write(payload.Bytes())
	_ = binary.Write(&out, binary.BigEndian, crc32.Checksum(payload.Bytes(), crcTable))
	return out.Bytes(), nil
}

func encodeValue(w io.Writer, colName string, t DataType, v any) error {
	switch t {
	case TypeInt64:
		n, ok := v.(int64)
		if !ok {
			return fmt.Errorf("%w: column %q expects int64, got %T", ErrTypeMismatch, colName, v)
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(n))
		_, _ = w.Write(b[:])
	case TypeFloat64:
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("%w: column %q expects float64, got %T", ErrTypeMismatch, colName, v)
		}
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], math.Float64bits(f))
		_, _ = w.Write(b[:])
	case TypeBool:
		b, ok := v.(bool)
		if !ok {
			return fmt.Errorf("%w: column %q expects bool, got %T", ErrTypeMismatch, colName, v)
		}
		if b {
			_, _ = w.Write([]byte{1})
		} else {
			_, _ = w.Write([]byte{0})
		}
	case TypeString:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("%w: column %q expects string, got %T", ErrTypeMismatch, colName, v)
		}
		var lb [4]byte
		binary.BigEndian.PutUint32(lb[:], uint32(len(s)))
		_, _ = w.Write(lb[:])
		_, _ = w.Write([]byte(s))
	default:
		return fmt.Errorf("%w: column %q has unknown type %d", ErrCorruptData, colName, t)
	}
	return nil
}

// segmentDecoder 基于 ReaderAt 做按需读列：未被请求的列完全不发起读取。
type segmentDecoder struct {
	ra   io.ReaderAt
	size int64
	h    segmentFileHeader

	statsOff int64 // 统计块 payload 偏移（statsLen=0 表示无统计块）
	statsLen int
}

const segmentFixedHeaderLen = 4 + 2 + 4 + 4 + 2

// openDecoder 读取并校验文件头与列目录。
// 兼容 v1（老库，无统计块）与 v2（带统计块）；其余版本是"本代码无法理解的
// 内容"，报 ErrUnsupportedVersion（与字节损坏的 ErrCorruptData 严格区分）。
func openDecoder(ra io.ReaderAt, size int64) (*segmentDecoder, segmentFileHeader, error) {
	if size < segmentFixedHeaderLen {
		return nil, segmentFileHeader{}, fmt.Errorf("%w: file too short (%d bytes)", ErrCorruptData, size)
	}
	head := make([]byte, segmentFixedHeaderLen)
	if err := readFullAt(ra, head, 0); err != nil {
		return nil, segmentFileHeader{}, corruptf("fixed header: %v", err)
	}
	if string(head[0:4]) != segmentMagic {
		return nil, segmentFileHeader{}, fmt.Errorf("%w: bad magic %q", ErrCorruptData, string(head[0:4]))
	}
	version := binary.BigEndian.Uint16(head[4:6])
	if version < segmentFormatVersionOldest || version > segmentFormatVersion {
		return nil, segmentFileHeader{}, fmt.Errorf("%w: %d (this build reads %d..%d)",
			ErrUnsupportedVersion, version, segmentFormatVersionOldest, segmentFormatVersion)
	}
	nCols := int(binary.BigEndian.Uint16(head[14:16]))
	h := segmentFileHeader{
		version:       version,
		schemaVersion: int(binary.BigEndian.Uint32(head[6:10])),
		rowCount:      int(binary.BigEndian.Uint32(head[10:14])),
	}

	// 统计块（仅 v2 起存在）：只读长度定位目录，payload 延迟到需要过滤
	// 跳过时再解析。v1 老段没有统计块，statsLen=0，过滤时自动退回完整扫描。
	pos := int64(segmentFixedHeaderLen)
	var statsOff int64
	var statsLen int
	if version >= 2 {
		var slb [4]byte
		if err := readFullAt(ra, slb[:], pos); err != nil {
			return nil, segmentFileHeader{}, corruptf("stats length: %v", err)
		}
		statsLen = int(binary.BigEndian.Uint32(slb[:]))
		statsOff = pos + 4
		pos = statsOff + int64(statsLen) + 4 // payload + crc
		if pos > size {
			return nil, segmentFileHeader{}, corruptf("stats block overruns file (end %d, size %d)", pos, size)
		}
	}

	h.dir = make([]dirEntry, 0, nCols)
	for i := 0; i < nCols; i++ {
		var lnb [1]byte
		if err := readFullAt(ra, lnb[:], pos); err != nil {
			return nil, segmentFileHeader{}, corruptf("directory entry %d name length: %v", i, err)
		}
		pos++
		nameLen := int(lnb[0])
		rest := make([]byte, nameLen+1+4)
		if err := readFullAt(ra, rest, pos); err != nil {
			return nil, segmentFileHeader{}, corruptf("directory entry %d body: %v", i, err)
		}
		pos += int64(len(rest))
		e := dirEntry{
			name:     string(rest[:nameLen]),
			typ:      DataType(rest[nameLen]),
			frameLen: int(binary.BigEndian.Uint32(rest[nameLen+1:])),
		}
		if e.frameLen < 2+1+4+4 { // nameLen u16 + type + count + crc 是最小负载
			return nil, segmentFileHeader{}, corruptf("directory entry %d frame length %d implausible", i, e.frameLen)
		}
		h.dir = append(h.dir, e)
	}

	if pos > size {
		return nil, segmentFileHeader{}, corruptf("directory overruns file (dir ends at %d, size %d)", pos, size)
	}
	h.dataStart = pos
	for i := range h.dir {
		if i == 0 {
			h.dir[i].offset = h.dataStart
		} else {
			h.dir[i].offset = h.dir[i-1].offset + int64(h.dir[i-1].frameLen)
		}
	}
	if last := len(h.dir) - 1; last >= 0 {
		end := h.dir[last].offset + int64(h.dir[last].frameLen)
		if end != size {
			return nil, segmentFileHeader{}, corruptf("frames do not fill file exactly (end %d, size %d)", end, size)
		}
	} else if pos != size {
		return nil, segmentFileHeader{}, corruptf("zero-column file has trailing bytes")
	}

	return &segmentDecoder{ra: ra, size: size, h: h, statsOff: statsOff, statsLen: statsLen}, h, nil
}

func (d *segmentDecoder) rowCount() int { return d.h.rowCount }

// loadStats 读取并解析统计块。统计块只是跳过依据：缺失、CRC 错或编码
// 损坏都返回 ok=false，调用方退回完整扫描，绝不上抛错误。
func (d *segmentDecoder) loadStats() (map[string]columnStats, bool) {
	if d.statsLen == 0 {
		return nil, false
	}
	buf := make([]byte, d.statsLen+4)
	if err := readFullAt(d.ra, buf, d.statsOff); err != nil {
		return nil, false
	}
	payload, crcBytes := buf[:d.statsLen], buf[d.statsLen:]
	if crc32.Checksum(payload, crcTable) != binary.BigEndian.Uint32(crcBytes) {
		return nil, false
	}
	return decodeStats(payload)
}

// loadColumns 只加载 wanted 中标记的列（wanted 以当前列名为键）。
//   - 存储列先经 resolveStoredColumn 按生命周期 ID 映射到当前列
//     （改名后按新名字归位；已删除/同名重生的旧列直接跳过，永不加载）；
//   - 当前结构有、文件里没有（后增列）：由调用方补 NULL；
//   - 存储类型无法安全转换：返回 ErrIncompatibleSchema；
//   - 帧损坏/截断/CRC 错：返回 ErrCorruptData。
func (d *segmentDecoder) loadColumns(target Schema, seg *Schema, wanted map[string]bool) (map[string]*decodedColumn, error) {
	out := make(map[string]*decodedColumn)
	for _, e := range d.h.dir {
		tcol, inTarget := resolveStoredColumn(seg, target, e.name)
		if !inTarget {
			continue // 已删除或同名重生的旧列：物理数据保留但永不加载
		}
		if !wanted[tcol.Name] {
			continue // 列裁剪：该列一帧都不读
		}
		frame := make([]byte, e.frameLen)
		if err := readFullAt(d.ra, frame, e.offset); err != nil {
			return nil, corruptf("column %q frame: %v", e.name, err)
		}
		payload, crcBytes := frame[:len(frame)-4], frame[len(frame)-4:]
		wantCRC := binary.BigEndian.Uint32(crcBytes)
		if got := crc32.Checksum(payload, crcTable); got != wantCRC {
			return nil, corruptf("column %q CRC mismatch (want %08x got %08x)", e.name, wantCRC, got)
		}
		col, err := decodeColumnPayload(e.name, payload, d.h.rowCount)
		if err != nil {
			return nil, err
		}
		// 目录与帧的类型交叉校验
		if col.typ != e.typ {
			return nil, corruptf("column %q type disagrees between directory (%s) and frame (%s)",
				e.name, e.typ, col.typ)
		}
		if !canConvert(col.typ, tcol.Type) {
			return nil, fmt.Errorf("%w: column %q stored as %s cannot be read as %s",
				ErrIncompatibleSchema, e.name, col.typ, tcol.Type)
		}
		if col.typ != tcol.Type { // int64 -> float64
			for i, v := range col.vals {
				if n, ok := v.(int64); ok {
					col.vals[i] = float64(n)
				}
			}
			col.typ = tcol.Type
		}
		out[tcol.Name] = col
	}
	return out, nil
}

func decodeColumnPayload(name string, payload []byte, rowCount int) (*decodedColumn, error) {
	if len(payload) < 2 {
		return nil, corruptf("column %q frame too short", name)
	}
	nl := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	if len(payload) < nl+1+4 {
		return nil, corruptf("column %q frame truncated in header", name)
	}
	pName := string(payload[:nl])
	typ := DataType(payload[nl])
	count := int(binary.BigEndian.Uint32(payload[nl+1 : nl+5]))
	payload = payload[nl+5:]
	if pName != name {
		return nil, corruptf("column frame identity mismatch (directory %q vs frame %q)", name, pName)
	}
	if count != rowCount {
		return nil, corruptf("column %q count %d != header row count %d", name, count, rowCount)
	}

	bitmapLen := (count + 7) / 8
	if len(payload) < bitmapLen {
		return nil, corruptf("column %q truncated in null bitmap", name)
	}
	bitmap, valueBytes := payload[:bitmapLen], payload[bitmapLen:]

	vals := make([]any, count)
	r := bytes.NewReader(valueBytes)
	for i := 0; i < count; i++ {
		if bitmap[i/8]&(1<<uint(i%8)) == 0 {
			vals[i] = nil
			continue
		}
		v, err := decodeValue(r, name, typ)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	if r.Len() != 0 {
		return nil, corruptf("column %q has %d trailing bytes after values", name, r.Len())
	}
	return &decodedColumn{typ: typ, vals: vals}, nil
}

func decodeValue(r *bytes.Reader, colName string, t DataType) (any, error) {
	switch t {
	case TypeInt64:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, corruptf("column %q int64 value: %v", colName, err)
		}
		return int64(binary.BigEndian.Uint64(b[:])), nil
	case TypeFloat64:
		var b [8]byte
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return nil, corruptf("column %q float64 value: %v", colName, err)
		}
		return math.Float64frombits(binary.BigEndian.Uint64(b[:])), nil
	case TypeBool:
		b, err := r.ReadByte()
		if err != nil {
			return nil, corruptf("column %q bool value: %v", colName, err)
		}
		switch b {
		case 0:
			return false, nil
		case 1:
			return true, nil
		default:
			return nil, corruptf("column %q invalid bool byte %d", colName, b)
		}
	case TypeString:
		var lb [4]byte
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			return nil, corruptf("column %q string length: %v", colName, err)
		}
		n := binary.BigEndian.Uint32(lb[:])
		sb := make([]byte, n)
		if _, err := io.ReadFull(r, sb); err != nil {
			return nil, corruptf("column %q string body: %v", colName, err)
		}
		return string(sb), nil
	default:
		return nil, corruptf("column %q unknown stored type %d", colName, t)
	}
}

func readFullAt(ra io.ReaderAt, buf []byte, off int64) error {
	n, err := ra.ReadAt(buf, off)
	if err == io.EOF && n == len(buf) {
		return nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n != len(buf) {
		return io.ErrUnexpectedEOF
	}
	return nil
}

func corruptf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrCorruptData, fmt.Sprintf(format, args...))
}
