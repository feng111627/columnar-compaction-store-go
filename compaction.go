package columnar

import (
	"context"
	"os"
	"path/filepath"
)

// compact 的崩溃与并发安全性：
//
//  1. 新合并段先写成独立文件 seg-<id>.ccs（经 .tmp + rename 落盘），
//     此刻任何读者都不知道它的存在。
//  2. 清单交换在 mu.Lock 下进行（一次性把旧段集合替换成新段），
//     并通过"临时文件 + fsync + 原子 rename"持久化；读者在 RLock 内
//     取快照，因此要么看到旧段集合、要么看到新段集合，绝不会看到半截。
//  3. 交换之后先等待所有持旧快照的扫描器 Close（readers.Wait），
//     才删除旧段文件，保证进行中的读不会碰到被删文件。
//  4. 崩溃窗口覆盖（重启后都必须回到一致状态，由 cleanCrashArtifacts 完成）：
//     a. 新段写完之前（.tmp 阶段）：临时文件被清理，状态=合并前；
//     b. 新段已完整落盘、清单尚未交换（failpoint compactFailAfterCommit）：
//        清单仍引用旧段，新段是无主文件被清理，状态=合并前；
//     c. 清单已交换、旧段未及删除（failpoint compactFailAfterSwap）：
//        清单只引用新段，旧段是无主文件被清理，状态=合并后；
//     交换前任一步骤失败同样回滚（删除新段、清单保持旧状态）。
//     三个窗口都不会出现行数对不上、半截数据或重复行。

// Compact 执行一次小文件合并。
func (s *Store) Compact(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.compacting {
		// 同一时刻只允许一个合并任务。
		return errInjected("compactionAlreadyRunning")
	}
	s.compacting = true
	defer func() { s.compacting = false }()
	return s.compactLocked(ctx)
}

func (s *Store) compactLocked(ctx context.Context) error {
	if err := s.flushLocked(ctx); err != nil {
		return err
	}

	picked := pickCompactionSegments(s.m, s.opt)
	if len(picked) < 2 {
		return nil // 没有可合并的小文件
	}

	schema := *s.m.currentSchema()
	newID := s.m.NextSegmentID
	sw, err := newSegmentWriter(s.dir, newID, schema)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = sw.Close()
			_ = os.Remove(sw.tmpPath())
		}
	}()

	totalRows := 0
	for _, meta := range picked {
		if err := ctx.Err(); err != nil {
			return err
		}
		rd, err := openSegment(s.dir, meta, schema, s.m.SchemaHistory)
		if err != nil {
			return err
		}
		rows, err := rd.scan(ctx, ScanOptions{})
		_ = rd.close()
		if err != nil {
			return err
		}
		for _, r := range rows {
			if err := sw.appendRow(r); err != nil {
				return err
			}
		}
		totalRows += len(rows)
	}

	// 故障注入点：新段写完、清单交换之前失败（用于验证回滚）。
	if s.failPointOn("compactFailBeforeSwap") {
		return errInjected("compactFailBeforeSwap")
	}

	newName, n, err := sw.commit(s.m.Version)
	if err != nil {
		return err
	}
	if n != totalRows {
		// 理论上不可能发生，发生即视为损坏，回滚。
		_ = os.Remove(filepath.Join(s.dir, newName))
		return errInjected("rowCountMismatch")
	}

	// 故障注入点：新段文件已完整落盘（.tmp -> rename 完成）、但清单尚未
	// 交换时进程被杀。模拟方式：直接返回；新段作为"无主段"留在磁盘上，
	// 清单仍是旧版，重启时由 cleanCrashArtifacts 清理，整体回到合并前。
	if s.failPointOn("compactFailAfterCommit") {
		return errInjected("compactFailAfterCommit")
	}

	// 构造交换后的清单：移除被合并段、追加新段，行数保持不变。
	remove := map[int64]bool{}
	for _, meta := range picked {
		remove[meta.ID] = true
	}
	newSegs := make([]segmentMeta, 0, len(s.m.Segments)-len(picked)+1)
	for _, meta := range s.m.Segments {
		if !remove[meta.ID] {
			newSegs = append(newSegs, meta)
		}
	}
	newSegs = append(newSegs, segmentMeta{
		ID:            newID,
		FileName:      newName,
		RowCount:      n,
		SchemaVersion: schema.Version,
	})
	cand := &manifest{
		Version:       s.m.Version + 1,
		SchemaHistory: s.m.SchemaHistory,
		Segments:      newSegs,
		NextSegmentID: newID + 1,
		NextColumnID:  s.m.NextColumnID,
	}

	// 原子持久化新清单；失败则回滚（删除新段，保留旧清单与旧段）。
	if err := saveManifest(s.dir, cand); err != nil {
		_ = os.Remove(filepath.Join(s.dir, newName))
		return err
	}
	oldPicked := picked
	s.m = cand
	committed = true
	_ = sw.Close()

	// 故障注入点：清单已交换、旧段尚未删除时"崩溃"。
	// 模拟方式：直接返回；旧段留在磁盘，重启时作为无主段被清理。
	if s.failPointOn("compactFailAfterSwap") {
		return errInjected("compactFailAfterSwap")
	}

	// 等待所有快照读者结束（新读者只会看到新清单），再物理删除旧段。
	// readers.Wait 不能在持锁时调用（读者释放不需要锁），这里锁仍持有，
	// 但 releaseReader 不获取 mu，因此不会死锁。
	s.readers.Wait()
	removeFiles(s.dir, fileNamesOf(oldPicked))
	return nil
}

// pickCompactionSegments 选出待合并的段：
//   - 存活段数达到 CompactionMinSegments 时，合并全部段（全量合并，
//     语义最简单也最容易给出确定的一致性证明）；
//   - 或者存在多个小于目标行数的小段时，合并这些小段
//     （CompactionTargetRows > 0 时生效）。
func pickCompactionSegments(m *manifest, opt Options) []segmentMeta {
	if len(m.Segments) < 2 {
		return nil
	}
	if len(m.Segments) >= opt.CompactionMinSegments {
		out := make([]segmentMeta, len(m.Segments))
		copy(out, m.Segments)
		return out
	}
	if opt.CompactionTargetRows > 0 {
		var small []segmentMeta
		for _, seg := range m.Segments {
			if seg.RowCount < opt.CompactionTargetRows {
				small = append(small, seg)
			}
		}
		if len(small) >= 2 {
			return small
		}
	}
	return nil
}

func fileNamesOf(metas []segmentMeta) []string {
	names := make([]string, 0, len(metas))
	for _, m := range metas {
		names = append(names, m.FileName)
	}
	return names
}

// removeFiles 尽力删除文件；删除失败不影响已提交的清单状态。
func removeFiles(dir string, names []string) {
	for _, name := range names {
		_ = os.Remove(filepath.Join(dir, name))
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
}
