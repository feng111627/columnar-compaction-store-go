package columnar

import (
	"encoding/json"
	"hash/crc32"
)

// statCRC 对 columnStat 除 CRC 字段外的内容做规范化 JSON 编码后取 CRC32。
// 字段集合固定、encoding/json 对固定结构的输出确定，因此同值的 CRC 稳定
// 且可在读取侧重算。
func statCRC(st columnStat) uint32 {
	st.CRC = 0
	b, err := json.Marshal(st)
	if err != nil {
		return 0
	}
	return crc32.Checksum(b, crcTable)
}

// statIntact 判断依据自身是否完好（CRC 自洽）。
func statIntact(st columnStat) bool {
	if st.Type == TypeInvalid {
		return false
	}
	return statCRC(st) == st.CRC
}

// 段级跳过（zone-map pruning）：
//
// 每个段在写入/合并时按"段写入时列名"为每列计算一份极值依据（非 NULL
// 值的 min/max 与 NULL 计数），随清单原子持久化。带过滤条件的扫描在打开
// 段文件之前先用依据判断该段是否可能命中：若某个过滤条件在该段的极值
// 范围上已被证明不可能为真，则整段跳过（段文件一个字节都不读），列裁剪
// 对未跳过的段照常生效。
//
// 安全回退优先于性能：依据缺失（段无 Stats 或该列无条目）、身份无法绑定
// （改名/重生场景下依据的名字无法对应到当前列）、类型无法比较、JSON 损坏
// 等任何"依据不可信"的情况，一律视为不能跳过。读到真实数据后还会与依据
// 交叉校验，发现依据与数据对不上（min/max 不包含实际值、NULL 计数不符）
// 立即放弃本轮跳过结果并用完整扫描重跑，宁可多读也绝不少行。

// buildSegmentStats 按 schema 列顺序计算 rows 的段级极值依据。
// 行已经按该 schema 规范化与类型校验，v==nil 表示 NULL。
func buildSegmentStats(rows []Row, schema Schema) map[string]columnStat {
	stats := make(map[string]columnStat, len(schema.Columns))
	for _, col := range schema.Columns {
		st := columnStat{Type: col.Type}
		seen := false
		for _, r := range rows {
			v := r[col.Name]
			if v == nil {
				st.NullCount++
				continue
			}
			switch n := v.(type) {
			case int64:
				if !seen {
					st.IntV, st.IntMax, seen = n, n, true
				} else {
					if n < st.IntV {
						st.IntV = n
					}
					if n > st.IntMax {
						st.IntMax = n
					}
				}
			case float64:
				if !seen {
					st.FltV, st.FltMax, seen = n, n, true
				} else {
					if n < st.FltV {
						st.FltV = n
					}
					if n > st.FltMax {
						st.FltMax = n
					}
				}
			case string:
				if !seen {
					st.StrMin, st.StrMax, seen = n, n, true
				} else {
					if n < st.StrMin {
						st.StrMin = n
					}
					if n > st.StrMax {
						st.StrMax = n
					}
				}
			case bool:
				if !seen {
					st.BoolMin, st.BoolMax, seen = n, n, true
				} else {
					if !n {
						st.BoolMin = false
					}
					if n {
						st.BoolMax = true
					}
				}
			}
		}
		stats[col.Name] = st
	}
	// 全部极值算完后统一计算 CRC（CRC 字段不参与自身输入）。
	for name, st := range stats {
		st.CRC = statCRC(st)
		stats[name] = st
	}
	return stats
}

// buildStatsKey 为一次扫描构造"当前列名 -> 段写入时依据名"的映射。
// 依据按段写入时列名存储：改名列经稳定身份映射回旧名（因此改名后跳过
// 判断仍然有效）；同名重生列在当前结构里是全新身份，写入时结构中不
// 存在该身份，映射失败（map 中无此键），从而对该列不跳过——绝不会拿
// 旧列依据错误地排除新列数据。
func buildStatsKey(writeSchema, target Schema) map[string]string {
	key := make(map[string]string)
	for _, tc := range target.Columns {
		if tc.ID == 0 {
			key[tc.Name] = tc.Name // 旧存储（身份特性上线前）：按名字
			continue
		}
		// 找写入时结构中身份相同的列。
		for _, wc := range writeSchema.Columns {
			if wc.ID == tc.ID {
				key[tc.Name] = wc.Name
				break
			}
		}
	}
	return key
}

// canSkipSegment 在真正读取段之前，依据 stats 判断该段是否一定不命中
// 全部过滤条件（AND）。任一过滤无法证明不命中（依据缺失/不可比）都不跳过。
func canSkipSegment(meta segmentMeta, writeSchema, target Schema, statsKey map[string]string, filters []Filter) bool {
	if len(filters) == 0 || meta.RowCount == 0 || meta.Stats == nil {
		return false
	}
	for _, f := range filters {
		key, ok := statsKey[f.Column]
		if !ok {
			return false // 身份无法绑定到段写入时的列
		}
		st, ok := meta.Stats[key]
		if !ok {
			return false // 该列无依据
		}
		if !statIntact(st) {
			return false // 依据 CRC 损坏/不自洽：安全回退，不跳过
		}
		if st.NullCount < 0 || st.NullCount > meta.RowCount {
			return false // 依据结构不自洽：不跳过
		}
		if st.NullCount == meta.RowCount {
			continue // 段内全 NULL：该比较在本段不可能为真，此条件已排除该段
		}
		tcol, ok := target.Column(f.Column)
		if !ok {
			return false
		}
		if !statCanExclude(st, tcol, f) {
			return false
		}
	}
	return true
}

// statCanExclude 判断单个过滤是否在极值依据上被证明不可能为真。
// 返回 false 表示"可能命中或无法证明"（段必须读）。
func statCanExclude(st columnStat, target Column, f Filter) bool {
	switch v := f.Value.(type) {
	case int64:
		if st.Type != TypeInt64 || target.Type != TypeInt64 {
			return false
		}
		return rangeExcludesOrdered(st.IntV, st.IntMax, f.Op, v)
	case float64:
		switch st.Type {
		case TypeFloat64:
			if target.Type != TypeFloat64 {
				return false
			}
			return rangeExcludesOrdered(st.FltV, st.FltMax, f.Op, v)
		case TypeInt64:
			// int64 -> float64 演进：把 int 极值提升为 float64 再比较。
			if target.Type != TypeFloat64 {
				return false
			}
			return rangeExcludesOrdered(float64(st.IntV), float64(st.IntMax), f.Op, v)
		default:
			return false
		}
	case string:
		if st.Type != TypeString || target.Type != TypeString {
			return false
		}
		return rangeExcludesOrdered(st.StrMin, st.StrMax, f.Op, v)
	case bool:
		if st.Type != TypeBool || target.Type != TypeBool {
			return false
		}
		if f.Op != OpEqual {
			return false // != 无法仅凭极值排除（段内可能同时含两种值）
		}
		if v {
			return !st.BoolMax // 段内没有任何 true（max=false）则 ==true 不命中
		}
		return st.BoolMin // 段内没有任何 false（min=true）则 ==false 不命中
	default:
		return false
	}
}

// ordered 约束有序类型（int64/float64/string）支持的六种操作符。
type ordered interface {
	int64 | float64 | string
}

func rangeExcludesOrdered[T ordered](min, max T, op FilterOp, v T) bool {
	switch op {
	case OpEqual:
		return v < min || v > max
	case OpNotEqual:
		// 极值无法证明"所有值都等于 v"，保守不排除。
		return false
	case OpLess: // 需要存在 x < v；min >= v 即 v <= min 时不可能
		return v <= min
	case OpLessEqual: // 需要存在 x <= v；min > v 即 v < min 时不可能
		return v < min
	case OpGreater: // 需要存在 x > v；max <= v 即 v >= max 时不可能
		return v >= max
	case OpGreaterEqual: // 需要存在 x >= v；max < v 即 v > max 时不可能
		return v > max
	default:
		return false
	}
}
