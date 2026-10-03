package columnar

import (
	"encoding/binary"
	"hash/crc32"
	"math"
)

// 列式块磁盘编码（详见 README“落盘布局”）：
//
//	body   := u32 nullBitmapLen | nullBitmap | payload
//	bitmap := 第 r 行位于 byte(r/8) 的 bit(r%8)，置 1 表示 NULL
//	payload（仅编码非 NULL 行，保持行顺序）：
//	  int64   8 字节小端
//	  float64 8 字节小端 IEEE-754
//	  bool    1 字节，取值 0/1
//	  string  uvarint 长度 + UTF-8 字节
//	外层 := u32(bodyLen) | body | u32(crc32c of body)
//
// 行数由数据文件头给出（decodeChunk 的 rows 参数），任何长度不足、校验不通过、
// 非法布尔/位图填充位非零/payload 残余都包装 ErrCorruptData，绝不静默跳过。
var crcTable = crc32.MakeTable(crc32.Castagnoli)

func putU32(b []byte, v uint32) { binary.LittleEndian.PutUint32(b, v) }
func putU64(b []byte, v uint64) { binary.LittleEndian.PutUint64(b, v) }
func u32(b []byte) uint32       { return binary.LittleEndian.Uint32(b) }
func u64(b []byte) uint64       { return binary.LittleEndian.Uint64(b) }

func encodeChunk(c *Column) ([]byte, error) {
	const op = "datafile.encode"
	if err := c.Valid(); err != nil {
		return nil, err
	}
	n := c.Rows()
	bitmapLen := (n + 7) / 8

	payloadCap := 0
	for r := 0; r < n; r++ {
		if c.Nulls[r] {
			continue
		}
		switch c.Type {
		case TypeInt64, TypeFloat64:
			payloadCap += 8
		case TypeBool:
			payloadCap++
		case TypeString:
			payloadCap += binary.MaxVarintLen64 + len(c.Strs[r])
		}
	}

	body := make([]byte, 4+bitmapLen+payloadCap)
	putU32(body[0:4], uint32(bitmapLen))
	bitmap := body[4 : 4+bitmapLen]
	payload := body[4+bitmapLen:]

	off := 0
	for r := 0; r < n; r++ {
		if c.Nulls[r] {
			bitmap[r/8] |= 1 << uint(r%8)
			continue
		}
		switch c.Type {
		case TypeInt64:
			putU64(payload[off:off+8], uint64(c.I64s[r]))
			off += 8
		case TypeFloat64:
			putU64(payload[off:off+8], math.Float64bits(c.F64s[r]))
			off += 8
		case TypeBool:
			if c.Bools[r] {
				payload[off] = 1
			}
			off++
		case TypeString:
			off += binary.PutUvarint(payload[off:], uint64(len(c.Strs[r])))
			off += copy(payload[off:], c.Strs[r])
		default:
			return nil, failf(op, ErrTypeMismatch, "列 %q 类型不可编码: %v", c.Name, c.Type)
		}
	}
	body = body[:4+bitmapLen+off] // 字符串 uvarint 预留可能偏大，截到真实长度

	out := make([]byte, 4+len(body)+4)
	putU32(out[0:4], uint32(len(body)))
	copy(out[4:], body)
	putU32(out[4+len(body):], crc32.Checksum(body, crcTable))
	return out, nil
}

func decodeChunk(data []byte, rows int, colName string, t ValueType) (*Column, error) {
	const op = "datafile.decode"
	if rows < 0 {
		return nil, failf(op, ErrCorruptData, "列 %q 行数为负: %d", colName, rows)
	}
	if len(data) < 12 {
		return nil, failf(op, ErrCorruptData, "列 %q 编码块过短: %d 字节", colName, len(data))
	}
	bodyLen := int(u32(data[0:4]))
	if bodyLen < 4 || 4+bodyLen+4 != len(data) {
		return nil, failf(op, ErrCorruptData, "列 %q 块长度字段 %d 与外层长度 %d 不一致", colName, bodyLen, len(data))
	}
	body := data[4 : 4+bodyLen]
	wantCRC := u32(data[4+bodyLen:])
	if got := crc32.Checksum(body, crcTable); got != wantCRC {
		return nil, failf(op, ErrCorruptData, "列 %q CRC32C 校验失败: 文件=%08x 计算=%08x", colName, wantCRC, got)
	}
	bitmapLen := (rows + 7) / 8
	// rows==0 时允许 bitmapLen=0。
	if bitmapLen == 0 && rows > 0 {
		return nil, failf(op, ErrCorruptData, "列 %q 行数与位图不匹配", colName)
	}
	if int(u32(body[0:4])) != bitmapLen {
		return nil, failf(op, ErrCorruptData, "列 %q 位图长度 %d 与行数 %d 不符",
			colName, u32(body[0:4]), rows)
	}
	if 4+bitmapLen > len(body) {
		return nil, failf(op, ErrCorruptData, "列 %q 位图超出块体", colName)
	}
	bitmap := body[4 : 4+bitmapLen]
	payload := body[4+bitmapLen:]

	col := &Column{Name: colName, Type: t}
	col.Nulls = make([]bool, rows)
	switch t {
	case TypeInt64:
		col.I64s = make([]int64, rows)
	case TypeFloat64:
		col.F64s = make([]float64, rows)
	case TypeBool:
		col.Bools = make([]bool, rows)
	case TypeString:
		col.Strs = make([]string, rows)
	default:
		return nil, failf(op, ErrTypeMismatch, "列 %q 出现未知类型 %v", colName, t)
	}

	for r := 0; r < rows; r++ {
		null := bitmap[r/8]&(1<<uint(r%8)) != 0
		col.Nulls[r] = null
		if null {
			continue
		}
		switch t {
		case TypeInt64:
			if len(payload) < 8 {
				return nil, failf(op, ErrCorruptData, "列 %q int64 数据在第 %d 行截断", colName, r)
			}
			col.I64s[r] = int64(u64(payload[:8]))
			payload = payload[8:]
		case TypeFloat64:
			if len(payload) < 8 {
				return nil, failf(op, ErrCorruptData, "列 %q float64 数据在第 %d 行截断", colName, r)
			}
			col.F64s[r] = math.Float64frombits(u64(payload[:8]))
			payload = payload[8:]
		case TypeBool:
			if len(payload) < 1 {
				return nil, failf(op, ErrCorruptData, "列 %q bool 数据在第 %d 行截断", colName, r)
			}
			switch payload[0] {
			case 0:
			case 1:
				col.Bools[r] = true
			default:
				return nil, failf(op, ErrCorruptData, "列 %q 第 %d 行 bool 编码非法: %d", colName, r, payload[0])
			}
			payload = payload[1:]
		case TypeString:
			slen, k := binary.Uvarint(payload)
			if k <= 0 {
				return nil, failf(op, ErrCorruptData, "列 %q 第 %d 行字符串长度编码损坏", colName, r)
			}
			payload = payload[k:]
			if uint64(len(payload)) < slen {
				return nil, failf(op, ErrCorruptData, "列 %q 第 %d 行字符串截断: 需要 %d 字节剩余 %d",
					colName, r, slen, len(payload))
			}
			col.Strs[r] = string(payload[:slen])
			payload = payload[slen:]
		}
	}
	if len(payload) != 0 {
		return nil, failf(op, ErrCorruptData, "列 %q 解码后 payload 残余 %d 字节", colName, len(payload))
	}
	// 位图最后一字节的填充位必须为 0，防止伪造行数。
	if pad := rows % 8; pad != 0 {
		last := bitmap[bitmapLen-1]
		for bit := pad; bit < 8; bit++ {
			if last&(1<<uint(bit)) != 0 {
				return nil, failf(op, ErrCorruptData, "列 %q 位图填充位非零", colName)
			}
		}
	}
	return col, nil
}
