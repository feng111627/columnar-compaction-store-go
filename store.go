package columnar

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Options 存储参数。
type Options struct {
	// FlushThreshold 内存攒够多少行落一个段文件。
	FlushThreshold int
	// CompactionMinSegments 存活段数达到该值才触发合并。
	CompactionMinSegments int
	// CompactionTargetRows 合并后单段的目标行数（0 表示不限）。
	CompactionTargetRows int
}

// DefaultOptions 默认参数。
func DefaultOptions() Options {
	return Options{
		FlushThreshold:        1000,
		CompactionMinSegments: 4,
		CompactionTargetRows:  0,
	}
}

// Store 一个本地列式存储（一个目录对应一张表）。
//
// 并发模型：
//   - mu(RWMutex) 保护内存中的 manifest 与写缓冲；快照扫描在 RLock 内冻结
//     段清单并登记引用，随后的磁盘读取不持锁。
//   - readers(WaitGroup) 计数正在读取旧快照文件的扫描器；合并完成清单交换后，
//     必须等旧扫描器全部 Close 才删除旧段文件，从而读者永远看不到半截结果、
//     也不会读到被删掉的文件。
type Store struct {
	dir string
	opt Options

	mu sync.RWMutex
	m  *manifest

	buf []Row // 尚未落盘的内存行

	compacting bool
	closed     bool

	// readers 计数持有某个快照的活动扫描器。
	readers sync.WaitGroup

	// failPoints 仅供测试注入故障。
	failPoints map[string]bool
}

// Create 在 dir 上创建新存储，初始结构为 schema。
func Create(dir string, schema Schema, opt Options) (*Store, error) {
	if err := validateSchemaShape(schema); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if _, err := os.Stat(manifestPath(dir)); err == nil {
		return nil, fmt.Errorf("columnar: store already exists at %s", dir)
	}
	schema.Version = 1
	m := &manifest{
		Version:       1,
		SchemaHistory: []Schema{schema},
		NextSegmentID: 1,
	}
	if err := saveManifest(dir, m); err != nil {
		return nil, err
	}
	return openExisting(dir, opt)
}

// Open 打开已有存储；启动时清理上次崩溃遗留的临时文件与无主段文件。
func Open(dir string, opt Options) (*Store, error) {
	if _, err := loadManifest(dir); err != nil {
		return nil, err
	}
	return openExisting(dir, opt)
}

func openExisting(dir string, opt Options) (*Store, error) {
	if err := cleanCrashArtifacts(dir); err != nil {
		return nil, err
	}
	m, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	for _, seg := range m.Segments {
		if _, err := os.Stat(filepath.Join(dir, seg.FileName)); err != nil {
			return nil, fmt.Errorf("%w: segment %s referenced by manifest is missing: %v",
				ErrCorruptData, seg.FileName, err)
		}
	}
	if opt.FlushThreshold <= 0 {
		opt = DefaultOptions()
	}
	s := &Store{
		dir:        dir,
		opt:        opt,
		m:          m,
		failPoints: map[string]bool{},
	}
	return s, nil
}

// cleanCrashArtifacts 删除未提交的临时文件，以及清单里不再引用的旧段
// （只可能来自"清单已交换、旧文件未及删除"的崩溃现场）。
func cleanCrashArtifacts(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	m, mErr := readManifestFile(manifestPath(dir))
	live := map[string]bool{}
	if mErr == nil {
		for _, seg := range m.Segments {
			live[seg.FileName] = true
		}
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() {
			continue
		}
		if strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
			continue
		}
		// 清单可读且该段不在存活集合中：崩溃遗留的旧段。
		if mErr == nil && isSegmentFileName(name) && !live[name] && name != manifestName {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return err
			}
		}
	}
	return nil
}

func isSegmentFileName(name string) bool {
	return strings.HasPrefix(name, "seg-") && strings.HasSuffix(name, ".ccs")
}

func segmentFileName(id int64) string { return fmt.Sprintf("seg-%010d.ccs", id) }

func segmentTempName(id int64) string { return fmt.Sprintf(".seg-%010d.ccs.tmp", id) }

func validateSchemaShape(schema Schema) error {
	if len(schema.Columns) == 0 {
		return fmt.Errorf("%w: schema must have at least one column", ErrIncompatibleSchema)
	}
	names := map[string]bool{}
	for _, c := range schema.Columns {
		if c.Name == "" {
			return fmt.Errorf("%w: column name must not be empty", ErrIncompatibleSchema)
		}
		if c.Type == TypeInvalid {
			return fmt.Errorf("%w: column %q has invalid type", ErrIncompatibleSchema, c.Name)
		}
		if names[c.Name] {
			return fmt.Errorf("%w: duplicate column %q", ErrIncompatibleSchema, c.Name)
		}
		names[c.Name] = true
	}
	return nil
}

// Close 落盘内存数据并关闭。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if err := s.flushLocked(context.Background()); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (s *Store) checkOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

// Schema 返回当前结构快照。
func (s *Store) Schema() Schema {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return *s.m.currentSchema()
}

// SegmentCount 返回存活段数量（测试/观测用）。
func (s *Store) SegmentCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.m.Segments)
}

// BufferedRows 返回内存中未落盘的行数（测试用）。
func (s *Store) BufferedRows() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.buf)
}

// commitSchemaLocked 追加新版本结构并原子更新清单（调用方持 mu.Lock）。
func (s *Store) commitSchemaLocked(next Schema) error {
	if err := validateSchemaShape(next); err != nil {
		return err
	}
	newHist := make([]Schema, 0, len(s.m.SchemaHistory)+1)
	newHist = append(newHist, s.m.SchemaHistory...)
	newHist = append(newHist, next)
	cand := &manifest{
		Version:       s.m.Version + 1,
		SchemaHistory: newHist,
		Segments:      s.m.Segments,
		NextSegmentID: s.m.NextSegmentID,
	}
	if err := saveManifest(s.dir, cand); err != nil {
		return err
	}
	s.m = cand
	return nil
}

// pinSnapshot 冻结当前段清单、结构与内存缓冲，并登记一个读引用
// （扫描器 Close 时 releaseReader 释放）。
func (s *Store) pinSnapshot() ([]segmentMeta, Schema, []Row) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	metas := make([]segmentMeta, len(s.m.Segments))
	copy(metas, s.m.Segments)
	schema := *s.m.currentSchema()
	rows := make([]Row, len(s.buf))
	for i, r := range s.buf {
		cp := make(Row, len(r))
		for k, v := range r {
			cp[k] = v
		}
		rows[i] = cp
	}
	s.readers.Add(1)
	return metas, schema, rows
}

func (s *Store) releaseReader() { s.readers.Done() }

// SetFailPoint 测试用故障注入开关。
func (s *Store) SetFailPoint(name string, on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failPoints[name] = on
}

func (s *Store) failPointOn(name string) bool {
	return s.failPoints[name]
}
