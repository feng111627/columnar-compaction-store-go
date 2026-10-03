package columnar

// 列的"生命周期身份"绑定：
//
// 物理段里的列帧只带列名，而列名会随演进而变化。读取时必须先确定一个物理
// 帧在"当前结构"里到底是哪一列（或已不属于任何现存列）：
//
//   - 当前列带有非零稳定 ID 时：在段写入时结构（writeSchema）里找到同名
//     物理列的 ID，再用该 ID 在当前结构里定位列。改名前后 ID 不变，因此
//     旧名字节会被绑定到新名字；删除后同名重生的列拿到的是全新 ID，旧帧
//     在当前结构里找不到同 ID 的列，绑定失败，读成 NULL 且不做任何类型
//     解释（新旧类型不兼容也只影响这一列）。
//   - ID 为 0（身份特性上线前创建的存储）：回退为按列名绑定，保持升级前
//     的历史行为（不兼容类型仍按旧规则报 ErrIncompatibleSchema）。

// frameBinding 描述一个物理列帧到当前结构列的绑定结果。
type frameBinding struct {
	storedName string // 段文件目录中的物理列名
	writeCol   Column // 段写入时结构里的列定义（带身份 ID）
	target     Column // 当前结构中的目标列
	bound      bool   // 是否成功绑定到某个现存列
}

// resolveFrameBindings 按段内目录顺序返回每个物理帧的绑定结果。
func resolveFrameBindings(frames []dirEntry, writeSchema, target Schema) []frameBinding {
	byID := make(map[int64]Column, len(target.Columns))
	for _, c := range target.Columns {
		if c.ID != 0 {
			byID[c.ID] = c
		}
	}
	out := make([]frameBinding, 0, len(frames))
	for _, e := range frames {
		b := frameBinding{storedName: e.name}
		wc, inWrite := writeSchema.Column(e.name)
		b.writeCol = wc
		switch {
		case inWrite && wc.ID != 0:
			if tc, ok := byID[wc.ID]; ok {
				b.target = tc
				b.bound = true
			}
		default:
			// 旧存储（无 ID）或目录与写时结构对不上时的保守回退：按名字找。
			if tc, ok := target.Column(e.name); ok {
				b.target = tc
				b.bound = true
			}
		}
		out = append(out, b)
	}
	return out
}
