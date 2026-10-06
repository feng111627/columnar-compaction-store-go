package columnar

import "errors"

// 可区分的错误原因：调用方可用 errors.Is 断言失败类别。
var (
	// ErrColumnNotFound 列不存在（已删除或从未定义）。
	ErrColumnNotFound = errors.New("columnar: column not found")
	// ErrTypeMismatch 值的 Go 类型与列声明类型不匹配。
	ErrTypeMismatch = errors.New("columnar: value type does not match schema")
	// ErrCorruptData 数据文件编码损坏（魔数/CRC/长度/类型不一致）。
	ErrCorruptData = errors.New("columnar: corrupt data file")
	// ErrIncompatibleSchema 结构变更不兼容，被明确拒绝。
	ErrIncompatibleSchema = errors.New("columnar: incompatible schema change")
	// ErrManifestCorrupt 清单文件损坏。
	ErrManifestCorrupt = errors.New("columnar: manifest corrupt")
	// ErrSnapshotModified 并发结构变更导致快照失效。
	ErrSnapshotModified = errors.New("columnar: snapshot invalidated by concurrent schema change")
	// ErrNotFound 存储目录未初始化。
	ErrNotFound = errors.New("columnar: store not initialized")
	// ErrUnsupportedVersion 存储由更新版本的程序写出，当前版本无法兼容读取。
	// 与 ErrCorruptData（文件被改坏）严格区分：打开时即报，绝不拖到读取时。
	ErrUnsupportedVersion = errors.New("columnar: unsupported storage version")
	// ErrClosed 存储已关闭。
	ErrClosed = errors.New("columnar: store closed")
)
