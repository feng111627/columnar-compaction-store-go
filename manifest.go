package columnar

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const manifestName = "manifest.json"

// manifest 清单的持久化内容。
type manifest struct {
	Version       int           `json:"version"`        // 单调递增的清单版本
	SchemaHistory []Schema      `json:"schema_history"` // 全部历史结构（旧段解码用）
	Segments      []segmentMeta `json:"segments"`       // 当前存活段（按写入先后有序）
	NextSegmentID int64         `json:"next_segment_id"`
	NextColumnID  int           `json:"next_column_id,omitempty"` // 下一个待分配的列生命周期 ID
}

func (m *manifest) currentSchema() *Schema {
	if len(m.SchemaHistory) == 0 {
		return nil
	}
	return &m.SchemaHistory[len(m.SchemaHistory)-1]
}

func (m *manifest) schemaAt(version int) (Schema, bool) {
	return schemaAtVersion(m.SchemaHistory, version)
}

// schemaAtVersion 在历史里查找指定版本的结构快照。
func schemaAtVersion(hist []Schema, version int) (Schema, bool) {
	for _, sch := range hist {
		if sch.Version == version {
			return sch, true
		}
	}
	return Schema{}, false
}

// needsIDNormalization 报告清单是否来自升级前的版本（列没有生命周期身份）：
// 任一历史结构里存在 ID 为 0 的列，或 NextColumnID 缺失。
func (m *manifest) needsIDNormalization() bool {
	if m.NextColumnID <= 0 {
		return true
	}
	for _, sch := range m.SchemaHistory {
		for _, c := range sch.Columns {
			if c.ID == 0 {
				return true
			}
		}
	}
	return false
}

// normalizeColumnIDs 为升级前的清单补列生命周期身份（原地修改，调用方负责
// 原子持久化）。规则：按结构版本顺序扫描，同名列共享同一个 ID——这精确保持
// 旧版"按名匹配"的读取语义，老段字节在升级前后的读法完全一致（包括老库
// 历史上发生过的同名重生）；只有升级之后新发生的删列再加列才会分配全新 ID
// （走"同名重生旧数据读 NULL"的新语义）。
func (m *manifest) normalizeColumnIDs() {
	byName := map[string]int{}
	next := m.NextColumnID
	if next <= 0 {
		next = 1
	}
	// 先登记已存在的 ID（防御混合清单：部分列有 ID、部分没有）。
	for _, sch := range m.SchemaHistory {
		for _, c := range sch.Columns {
			if c.ID == 0 {
				continue
			}
			byName[c.Name] = c.ID
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
			if id, ok := byName[c.Name]; ok {
				c.ID = id
				continue
			}
			c.ID = next
			byName[c.Name] = next
			next++
		}
	}
	m.NextColumnID = next
}

// loadManifest 读取并校验清单；不存在返回包装了 ErrNotFound 的错误，
// 损坏（JSON 非法或内容不自洽）返回包装了 ErrManifestCorrupt 的错误。
func loadManifest(dir string) (*manifest, error) {
	m, err := readManifestFile(manifestPath(dir))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, manifestPath(dir))
		}
		return nil, fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func (m *manifest) validate() error {
	if m.currentSchema() == nil {
		return fmt.Errorf("%w: schema history is empty", ErrManifestCorrupt)
	}
	seen := make(map[int]bool, len(m.SchemaHistory))
	var lastVer int
	for i, sch := range m.SchemaHistory {
		if seen[sch.Version] {
			return fmt.Errorf("%w: duplicate schema version %d", ErrManifestCorrupt, sch.Version)
		}
		seen[sch.Version] = true
		if i > 0 && sch.Version <= lastVer {
			return fmt.Errorf("%w: schema versions not strictly increasing", ErrManifestCorrupt)
		}
		lastVer = sch.Version
		names := map[string]bool{}
		ids := map[int]bool{}
		for _, c := range sch.Columns {
			if c.Name == "" || c.Type == TypeInvalid {
				return fmt.Errorf("%w: invalid column in schema v%d", ErrManifestCorrupt, sch.Version)
			}
			if names[c.Name] {
				return fmt.Errorf("%w: duplicate column %q in schema v%d", ErrManifestCorrupt, c.Name, sch.Version)
			}
			names[c.Name] = true
			if c.ID < 0 {
				return fmt.Errorf("%w: negative column id %d in schema v%d", ErrManifestCorrupt, c.ID, sch.Version)
			}
			if c.ID != 0 {
				if ids[c.ID] {
					return fmt.Errorf("%w: duplicate column id %d in schema v%d", ErrManifestCorrupt, c.ID, sch.Version)
				}
				ids[c.ID] = true
			}
		}
	}
	segIDs := map[int64]bool{}
	for _, seg := range m.Segments {
		if segIDs[seg.ID] {
			return fmt.Errorf("%w: duplicate segment id %d", ErrManifestCorrupt, seg.ID)
		}
		segIDs[seg.ID] = true
		if seg.FileName == "" || seg.RowCount < 0 {
			return fmt.Errorf("%w: invalid segment entry %+v", ErrManifestCorrupt, seg)
		}
		if _, ok := m.schemaAt(seg.SchemaVersion); !ok {
			return fmt.Errorf("%w: segment %d references unknown schema version %d",
				ErrManifestCorrupt, seg.ID, seg.SchemaVersion)
		}
	}
	return nil
}

// saveManifest 以"临时文件 + fsync + 原子 rename + 目录 fsync"持久化清单，
// 保证任何时刻磁盘上的清单要么是旧版要么是新版。
func saveManifest(dir string, m *manifest) error {
	if err := m.validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	final := manifestPath(dir)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// readManifestFile 纯读取逻辑。
func readManifestFile(path string) (*manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrManifestCorrupt, err)
	}
	return &m, nil
}

func manifestPath(dir string) string { return filepath.Join(dir, manifestName) }
