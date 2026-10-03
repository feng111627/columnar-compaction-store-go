package columnar

import (
	"errors"
	"fmt"
)

// 可被 errors.Is 断言区分的错误类别。上层测试通过这些哨兵错误区分拒绝原因：
//   - ErrColumnNotFound     : 列不存在（读取投影列 / 写入多余列 / 结构演进目标缺失）
//   - ErrTypeMismatch       : 类型对不上（批次列类型、过滤字面量类型、读取转换失败）
//   - ErrCorruptData        : 落盘数据损坏（魔数、校验和、截断、非法编码）
//   - ErrIncompatibleChange : 不兼容的表结构变更（无法安全转换的类型变化等）
//   - ErrInvalidArgument    : 调用方参数非法
var (
	ErrColumnNotFound     = errors.New("columnar: column not found")
	ErrTypeMismatch       = errors.New("columnar: type mismatch")
	ErrCorruptData        = errors.New("columnar: corrupt data")
	ErrIncompatibleChange = errors.New("columnar: incompatible schema change")
	ErrInvalidArgument    = errors.New("columnar: invalid argument")
)

// Error 是组件统一返回的带上下文错误，Unwrap 同时暴露类别与底层原因，
// 因此 errors.Is(err, ErrCorruptData) 与 errors.Is(err, cause) 都成立。
type Error struct {
	Op     string // 产生错误的操作，如 "datafile.read"
	Kind   error  // 必为上面的哨兵错误之一
	Path   string // 相关文件路径（可选）
	Column string // 相关列名（可选）
	Detail string // 人读细节
	Cause  error  // 底层错误（可选）
}

func (e *Error) Error() string {
	s := "columnar"
	if e.Op != "" {
		s += " " + e.Op
	}
	if e.Kind != nil {
		s += ": " + e.Kind.Error()
	}
	if e.Path != "" {
		s += " [file=" + e.Path + "]"
	}
	if e.Column != "" {
		s += " [column=" + e.Column + "]"
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	if e.Cause != nil {
		s += ": " + e.Cause.Error()
	}
	return s
}

// Unwrap 同时暴露错误类别和底层 cause，支持 errors.Is / errors.As。
func (e *Error) Unwrap() []error {
	if e.Cause != nil && e.Kind != nil {
		return []error{e.Kind, e.Cause}
	}
	if e.Kind != nil {
		return []error{e.Kind}
	}
	return nil
}

func failf(op string, kind error, format string, args ...any) *Error {
	return &Error{Op: op, Kind: kind, Detail: fmt.Sprintf(format, args...)}
}

func failc(op string, kind error, cause error, format string, args ...any) *Error {
	return &Error{Op: op, Kind: kind, Cause: cause, Detail: fmt.Sprintf(format, args...)}
}
