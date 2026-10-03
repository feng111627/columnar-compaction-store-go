package columnar

import (
	"bytes"
	"encoding/binary"
	"math"
)

// 段级跳过（zone map）：每个段文件自带一份"列 -> min/max"统计块，
// 与数据帧一起编码落盘并受 CRC 保护。带过滤条件的扫描先读统计块，
// 若某个过滤条件对该段的所有行都必然为假，则整段跳过（一个数据帧都不读）。
//
// 安全回退策略（宁可多读，绝不漏行）：
//   - 统计块缺失（statsLen=0）、CRC 错、编码损坏、min>max、类型对不上：
//     一律当作"无统计"，退回完整扫描，绝不影响结果正确性；
//   - 统计只覆盖非 NULL 值（NULL 对任何比较都不为真，不影响跳过判定）；
//   - 含 NaN 的列不生成统计（NaN 的比较语义无法安全用于跳过）；
//   - 统计类型与过滤值类型不一致（如 int64->float64 演进后的旧段）：不跳。
// 统计在编码段文件时从真实行计算，合并重写段时同样重新计算，
// 因此统计与数据天然一致；不一致只可能来自物理损坏，由 CRC 兜底。

// columnStats 单列的非 NULL min/max。
type columnStats struct {
	typ DataType
	min any
	max any
}

// computeStats 从待编码的行计算每列 min/max。全 NULL 列与含 NaN 的列不出现在结果里。
func computeStats(schema Schema, rows []Row) map[string]columnStats {
	out := make(map[string]columnStats, len(schema.Columns))
	for _, col := range schema.Columns {
		var mn, mx any
		bad := false
		for _, r := range rows {
			v := r[col.Name]
			if v == nil {
				continue
			}
			if f, ok := v.(float64); ok && math.IsNaN(f) {
				bad = true // NaN 无法安全参与跳过判定，放弃该列统计
				break
			}
			if mn == nil {
				mn, mx = v, v
				continue
			}
			if c, err := compareValues(v, mn); err == nil && c < 0 {
				mn = v
			}
			if c, err := compareValues(v, mx); err == nil && c > 0 {
				mx = v
			}
		}
		if bad || mn == nil {
			continue
		}
		out[col.Name] = columnStats{typ: col.Type, min: mn, max: mx}
	}
	return out
}

// encodeStats 统计块负载编码（大端）：
//
//	nEntries u16
//	每项: nameLen u8 | name | type u8 | minValue | maxValue   （值编码同列帧，非 NULL）
func encodeStats(stats map[string]columnStats, order []Column) []byte {
	var buf bytes.Buffer
	n := 0
	for _, c := range order {
		if _, ok := stats[c.Name]; ok {
			n++
		}
	}
	_ = binary.Write(&buf, binary.BigEndian, uint16(n))
	for _, c := range order {
		st, ok := stats[c.Name]
		if !ok {
			continue
		}
		nb := []byte(c.Name)
		buf.WriteByte(byte(len(nb)))
		buf.Write(nb)
		buf.WriteByte(byte(st.typ))
		_ = encodeValue(&buf, c.Name, st.typ, st.min)
		_ = encodeValue(&buf, c.Name, st.typ, st.max)
	}
	return buf.Bytes()
}

// decodeStats 严格解码统计块；任何不合法都返回 ok=false（调用方退回全扫）。
func decodeStats(payload []byte) (map[string]columnStats, bool) {
	if len(payload) < 2 {
		return nil, false
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	r := bytes.NewReader(payload[2:])
	out := make(map[string]columnStats, n)
	for i := 0; i < n; i++ {
		lb, err := r.ReadByte()
		if err != nil {
			return nil, false
		}
		name := make([]byte, int(lb))
		if _, err := readFull(r, name); err != nil {
			return nil, false
		}
		tb, err := r.ReadByte()
		if err != nil {
			return nil, false
		}
		typ := DataType(tb)
		if typ <= TypeInvalid || typ > TypeBool {
			return nil, false
		}
		mn, err := decodeValue(r, string(name), typ)
		if err != nil {
			return nil, false
		}
		mx, err := decodeValue(r, string(name), typ)
		if err != nil {
			return nil, false
		}
		if c, err := compareValues(mn, mx); err != nil || c > 0 {
			return nil, false // min>max 或类型异常：统计不可信
		}
		out[string(name)] = columnStats{typ: typ, min: mn, max: mx}
	}
	if r.Len() != 0 {
		return nil, false
	}
	return out, true
}

func readFull(r *bytes.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// statsMayMatch 判断"该段是否可能含有命中 filter 的行"。
// 返回 false 表示该过滤条件对段内所有行必然为假，整段可以安全跳过；
// 任何不确定（无统计/类型不符/比较失败）都返回 true（不跳）。
func statsMayMatch(stats map[string]columnStats, storedName string, f Filter) bool {
	st, ok := stats[storedName]
	if !ok {
		return true
	}
	if !goTypeMatches(st.typ, f.Value) {
		return true // 统计类型与过滤值类型不一致（如类型演进后的旧段）：保守不跳
	}
	cMin, err1 := compareValues(f.Value, st.min)
	cMax, err2 := compareValues(f.Value, st.max)
	if err1 != nil || err2 != nil {
		return true
	}
	switch f.Op {
	case OpEqual:
		// v < min 或 v > max 时无行命中
		return cMin >= 0 && cMax <= 0
	case OpNotEqual:
		// 所有非 NULL 值都等于 v（min==max==v）时 != 无行命中
		eq, err := compareValues(st.min, st.max)
		if err != nil {
			return true
		}
		return !(eq == 0 && cMin == 0)
	case OpLess:
		// min >= v 时无行满足 col < v
		return cMin > 0
	case OpLessEqual:
		// min > v 时无行满足 col <= v
		return cMin >= 0
	case OpGreater:
		// max <= v 时无行满足 col > v
		return cMax < 0
	case OpGreaterEqual:
		// max < v 时无行满足 col >= v
		return cMax <= 0
	default:
		return true
	}
}
