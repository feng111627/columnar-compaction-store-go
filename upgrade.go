package columnar

// 老版本存储的在线升级（归一化）。
//
// 升级前版本写出的清单有两个特征：
//   - 结构历史里的列没有生命周期 ID（JSON 里无该字段，读入为 0）；
//   - 没有 next_column_id 字段（读入为 0）。
//
// 这种清单下读取会退回"按名称匹配"： plain 读没有问题，但改名会让老段
// 数据按旧名找不到列而逻辑丢失，删列再加同名列也无法获得"同名重生读 NULL"
// 的新语义。因此打开时做一次确定性的归一化：
//
//   - 按结构历史版本顺序、列顺序，为每个无 ID 的列分配生命周期 ID；
//   - 同名列共享同一个 ID —— 升级前的读取语义就是按名称匹配，同名即同列，
//     这样升级前后任何读取方式下老数据的行数与取值都完全一致；
//   - 归一化只发生在"首次用新版本打开老库"时，结果原子写回清单；
//     分配规则确定，崩溃后重开重复执行结果相同（幂等）。
//
// 归一化之后，该目录与全新创建的目录语义完全一致：改名连续可读、
// 同名重生读 NULL、后续新列从 NextColumnID 继续分配。
func normalizeLegacyManifest(m *manifest) bool {
	need := m.NextColumnID <= 0
	if !need {
		for _, sch := range m.SchemaHistory {
			for _, c := range sch.Columns {
				if c.ID == 0 {
					need = true
					break
				}
			}
			if need {
				break
			}
		}
	}
	if !need {
		return false
	}

	// 同名 -> 同一生命周期 ID。先收集已分配的 ID（兼容手工构造的
	// 部分带 ID 清单），再按历史顺序为无 ID 列回填。
	idByName := map[string]int{}
	next := 1
	for _, sch := range m.SchemaHistory {
		for _, c := range sch.Columns {
			if c.ID == 0 {
				continue
			}
			if _, ok := idByName[c.Name]; !ok {
				idByName[c.Name] = c.ID
			}
			if c.ID >= next {
				next = c.ID + 1
			}
		}
	}
	for i := range m.SchemaHistory {
		for j := range m.SchemaHistory[i].Columns {
			c := &m.SchemaHistory[i].Columns[j]
			if c.ID != 0 {
				continue
			}
			id, ok := idByName[c.Name]
			if !ok {
				id = next
				next++
				idByName[c.Name] = id
			}
			c.ID = id
		}
	}
	m.NextColumnID = next
	return true
}
