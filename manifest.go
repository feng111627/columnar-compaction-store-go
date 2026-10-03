package columnar

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Manifest 是某一时刻表的不可变元数据快照：文件列表 + 全部 schema 历史。
// 读取器持有某个 Manifest 后，其读取结果不受后续写入/合并影响（快照隔离）。
type Manifest struct {
	Table     string      `json:"table"`
	ID        int64       `json:"id"`
	Files     []FileEntry `json:"files"`
	Schemas   []schemaDTO `json:"schemas"`
	NextColID int         `json:"nextColID"`
	CreatedAt time.Time   `json:"createdAt"`

	// resolved 为反序列化后重建的内存结构；直接构造时由 buildResolved 填充。
	resolved []*Schema
}

// schemaDTO 是 Schema 的 JSON 表示（Schema 的索引字段不参与序列化）。
type schemaDTO struct {
	Table   string       `json:"table"`
	Version int          `json:"version"`
	Columns []*ColumnDef `json:"columns"`
}

func toDTO(s *Schema) schemaDTO {
	return schemaDTO{Table: s.Table, Version: s.Version, Columns: s.Columns}
}

func (m *Manifest) schemaHistory() (*Schemas, error) {
	hs := &Schemas{Table: m.Table, NextColID: m.NextColID}
	hs.versions = make([]*Schema, 0, len(m.resolved))
	for _, s := range m.resolved {
		cp, err := NewSchema(s.Table, s.Version, defsFromSchema(s))
		if err != nil {
			return nil, err
		}
		hs.versions = append(hs.versions, cp)
	}
	if len(hs.versions) == 0 {
		return nil, failf("manifest.load", ErrCorruptData, "表 %s 的 manifest 不含任何 schema 版本", m.Table)
	}
	return hs, nil
}

func defsFromSchema(s *Schema) []ColumnDef {
	out := make([]ColumnDef, 0, len(s.Columns))
	for _, c := range s.Columns {
		out = append(out, *c)
	}
	return out
}

func buildManifest(id int64, table string, files []FileEntry, hs *Schemas, now time.Time) *Manifest {
	m := &Manifest{
		Table:     table,
		ID:        id,
		Files:     append([]FileEntry(nil), files...),
		NextColID: hs.NextColID,
		CreatedAt: now,
	}
	m.Schemas = make([]schemaDTO, 0, len(hs.versions))
	for _, s := range hs.versions {
		// 关键：每个 manifest 快照必须持有独立的 schema 副本。
		// 否则后续 DDL 产生的新版本会与旧 manifest 共享底层结构，
		// 破坏快照隔离并让历史版本被就地污染。
		cp, err := NewSchema(s.Table, s.Version, defsFromSchema(s))
		if err != nil {
			// hs 中的版本构造时已经校验过，这里不应失败。
			panic(err)
		}
		m.Schemas = append(m.Schemas, toDTO(cp))
		m.resolved = append(m.resolved, cp)
	}
	sort.SliceStable(m.Files, func(i, j int) bool { return m.Files[i].ID < m.Files[j].ID })
	return m
}

func (m *Manifest) resolve() error {
	const op = "manifest.load"
	if m.Table == "" {
		return failf(op, ErrCorruptData, "manifest 表名为空")
	}
	if len(m.Schemas) == 0 {
		return failf(op, ErrCorruptData, "manifest %d 不含 schema 历史", m.ID)
	}
	m.resolved = make([]*Schema, 0, len(m.Schemas))
	seenVersion := map[int]bool{}
	for i, dto := range m.Schemas {
		if dto.Version != i+1 {
			return failf(op, ErrCorruptData, "manifest %d schema 版本不连续: 位置 %d 拿到版本 %d", m.ID, i+1, dto.Version)
		}
		if seenVersion[dto.Version] {
			return failf(op, ErrCorruptData, "manifest %d schema 版本重复 %d", m.ID, dto.Version)
		}
		seenVersion[dto.Version] = true
		cols := make([]ColumnDef, 0, len(dto.Columns))
		for _, c := range dto.Columns {
			cols = append(cols, *c)
		}
		s, err := NewSchema(dto.Table, dto.Version, cols)
		if err != nil {
			return failc(op, ErrCorruptData, err, "manifest %d 版本 %d 非法", m.ID, dto.Version)
		}
		m.resolved = append(m.resolved, s)
	}
	ids := map[int64]bool{}
	for _, f := range m.Files {
		if f.ID == 0 || f.Path == "" {
			return failf(op, ErrCorruptData, "manifest %d 文件条目不完整: %+v", m.ID, f)
		}
		if ids[f.ID] {
			return failf(op, ErrCorruptData, "manifest %d 文件 ID 重复: %d", m.ID, f.ID)
		}
		ids[f.ID] = true
		if f.SchemaVersion < 1 || f.SchemaVersion > len(m.Schemas) {
			return failf(op, ErrCorruptData, "manifest %d 文件 %d 引用未知 schema 版本 %d", m.ID, f.ID, f.SchemaVersion)
		}
	}
	sort.SliceStable(m.Files, func(i, j int) bool { return m.Files[i].ID < m.Files[j].ID })
	return nil
}

// manifestStore 负责 manifest 的原子持久化：
//
//	<dir>/tables/<table>/MANIFEST-<id>  完整 JSON 快照（临时文件 + rename）
//	<dir>/tables/<table>/CURRENT        UTF-8 文本 "MANIFEST-<id>\n"（原子替换）
//
// CURRENT 与 MANIFEST 各自都是全量原子替换，因此读取端要么看到旧完整快照、
// 要么看到新完整快照，不会观察到中间态。
type manifestStore struct {
	dir   string
	table string
}

func newManifestStore(dir, table string) *manifestStore {
	return &manifestStore{dir: dir, table: table}
}

func (ms *manifestStore) tableDir() string {
	return filepath.Join(ms.dir, "tables", ms.table)
}

func (ms *manifestStore) manifestPath(id int64) string {
	return filepath.Join(ms.tableDir(), fmt.Sprintf("MANIFEST-%08d", id))
}

func (ms *manifestStore) currentPath() string {
	return filepath.Join(ms.tableDir(), "CURRENT")
}

// Load 读取 CURRENT 指向的 manifest；若表目录尚不存在，返回 (nil, nil) 表示
// 一张尚未初始化的新表。CURRENT 悬空、JSON 损坏等均报 ErrCorruptData。
func (ms *manifestStore) Load() (*Manifest, error) {
	const op = "manifest.load"
	curBytes, err := os.ReadFile(ms.currentPath())
	if os.IsNotExist(err) {
		if _, statErr := os.Stat(ms.tableDir()); os.IsNotExist(statErr) {
			return nil, nil
		}
		// 目录存在但 CURRENT 缺失：可能是半成品初始化，按损坏上报。
		return nil, failf(op, ErrCorruptData, "目录 %s 存在但 CURRENT 缺失", ms.tableDir())
	} else if err != nil {
		return nil, failc(op, ErrCorruptData, err, "读取 CURRENT 失败")
	}
	name := strings.TrimSpace(string(curBytes))
	if name == "" {
		return nil, failf(op, ErrCorruptData, "CURRENT 内容为空")
	}
	mp := filepath.Join(ms.tableDir(), name)
	data, err := os.ReadFile(mp)
	if err != nil {
		return nil, failc(op, ErrCorruptData, err, "CURRENT 指向的 manifest 无法读取: %s", name)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, failc(op, ErrCorruptData, err, "manifest JSON 损坏: %s", name)
	}
	if err := m.resolve(); err != nil {
		return nil, err
	}
	if mp != ms.manifestPath(m.ID) {
		return nil, failf(op, ErrCorruptData, "manifest 文件名 %s 与内部 ID %d 不一致", name, m.ID)
	}
	return &m, nil
}

// Save 原子写出新 manifest 并推进 CURRENT。
func (ms *manifestStore) Save(m *Manifest) error {
	const op = "manifest.save"
	if err := os.MkdirAll(ms.tableDir(), 0o755); err != nil {
		return failc(op, ErrInvalidArgument, err, "创建表目录失败: %s", ms.tableDir())
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return failc(op, ErrCorruptData, err, "manifest 序列化失败")
	}
	if err := writeFileAtomic(ms.manifestPath(m.ID), data); err != nil {
		return err
	}
	curContent := filepath.Base(ms.manifestPath(m.ID)) + "\n"
	if err := writeFileAtomic(ms.currentPath(), []byte(curContent)); err != nil {
		return err
	}
	return nil
}

// RemoveOldManifests 删除除 keepID 外的历史 MANIFEST 文件。
// 安全性：读取器在构造期就已把 manifest 解析进内存，数据文件路径不再依赖
// MANIFEST 文件；且换入窗口保证没有读取器正处于构造中，因此旧清单可立即回收。
// 单个文件删除失败不影响正确性（只是残留），故仅返回最后一个错误供诊断。
func (ms *manifestStore) RemoveOldManifests(keepID int64) error {
	entries, err := os.ReadDir(ms.tableDir())
	if err != nil {
		return err
	}
	keep := filepath.Base(ms.manifestPath(keepID))
	var lastErr error
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "MANIFEST-") || name == keep {
			continue
		}
		if err := os.Remove(filepath.Join(ms.tableDir(), name)); err != nil {
			lastErr = err
		}
	}
	return lastErr
}
