package columnar

import (
	"context"
	"fmt"
)

// 结构演进的确定语义：
//   - 新增列：旧行在该列读出为 NULL；新列必须 Nullable（旧数据没有值）。
//   - 删除列：列定义从当前结构移除；旧段里的物理字节保留但永不对外可见。
//   - 类型变更：当前仅允许 int64 -> float64（精确无损地转成 float64，
//     |x| < 2^53 时值严格相同），其余跨类型变更一律拒绝。
// 每次演进都会产生一个新版本并追加到 SchemaHistory，旧段按写入时版本解码后
// 统一 evolveRow 到当前结构，因此新旧数据混读语义稳定。

// AddColumn 新增列（旧数据该列读出为 NULL）。新列总是获得一个全新的稳定
// 身份 ID；因此"先 DropColumn(name) 再 AddColumn(同名)"就是同名重生：
// 旧段里的同名字节属于已删除的旧身份，不会被当成新列读回。
func (s *Store) AddColumn(ctx context.Context, c Column) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(ctx); err != nil {
		return err
	}
	next, err := addColumn(*s.m.currentSchema(), c, s.m.NextColumnID)
	if err != nil {
		return err
	}
	s.m.NextColumnID++
	return s.commitSchemaLocked(next)
}

// RenameColumn 把列 oldName 改名为 newName，沿用同一个生命周期身份 ID。
// 改名前写入的历史段按旧名字节存储，读取时靠 ID 绑定到新名字，历史数据
// 连续可读；改名后必须以新名字写入与查询，旧名字不再对外存在。
func (s *Store) RenameColumn(ctx context.Context, oldName, newName string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(ctx); err != nil {
		return err
	}
	next, err := renameColumn(*s.m.currentSchema(), oldName, newName)
	if err != nil {
		return err
	}
	if next.Version == s.m.currentSchema().Version {
		return nil // 无实际变化（改成自己），不产生新版本
	}
	return s.commitSchemaLocked(next)
}

// DropColumn 删除列（旧段中该列数据不再对外可见）。
func (s *Store) DropColumn(ctx context.Context, name string) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(ctx); err != nil {
		return err
	}
	next, err := dropColumn(*s.m.currentSchema(), name)
	if err != nil {
		return err
	}
	return s.commitSchemaLocked(next)
}

// AlterColumnType 可兼容的类型变更（目前仅允许 int64 -> float64）。
func (s *Store) AlterColumnType(ctx context.Context, name string, t DataType) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(ctx); err != nil {
		return err
	}
	next, err := alterType(*s.m.currentSchema(), name, t)
	if err != nil {
		return err
	}
	if next.Version == s.m.currentSchema().Version {
		return nil // 幂等：无实际变化，不产生新版本
	}
	return s.commitSchemaLocked(next)
}

// addColumn 纯函数结构变换。newID 是分配给新列的生命周期身份。
func addColumn(sch Schema, c Column, newID int64) (Schema, error) {
	if c.Name == "" {
		return sch, fmt.Errorf("%w: column name must not be empty", ErrIncompatibleSchema)
	}
	if c.Type == TypeInvalid {
		return sch, fmt.Errorf("%w: column %q has invalid type", ErrIncompatibleSchema, c.Name)
	}
	if _, exists := sch.Column(c.Name); exists {
		return sch, fmt.Errorf("%w: column %q already exists", ErrIncompatibleSchema, c.Name)
	}
	if !c.Nullable {
		return sch, fmt.Errorf("%w: new column %q must be nullable because existing rows have no value",
			ErrIncompatibleSchema, c.Name)
	}
	c.ID = newID
	cols := make([]Column, 0, len(sch.Columns)+1)
	cols = append(cols, sch.Columns...)
	cols = append(cols, c)
	return Schema{Version: sch.Version + 1, Columns: cols}, nil
}

// renameColumn 纯函数结构变换：列的身份 ID 与类型/可空性保持不变，只换名字。
func renameColumn(sch Schema, oldName, newName string) (Schema, error) {
	if newName == "" {
		return sch, fmt.Errorf("%w: new column name must not be empty", ErrIncompatibleSchema)
	}
	col, ok := sch.Column(oldName)
	if !ok {
		return sch, fmt.Errorf("%w: cannot rename unknown column %q", ErrColumnNotFound, oldName)
	}
	if newName == oldName {
		return sch, nil // 幂等：无实际变化
	}
	if _, taken := sch.Column(newName); taken {
		return sch, fmt.Errorf("%w: cannot rename %q to %q: target name already exists",
			ErrIncompatibleSchema, oldName, newName)
	}
	cols := make([]Column, len(sch.Columns))
	for i, c := range sch.Columns {
		if c.Name == oldName {
			c.Name = newName
		}
		cols[i] = c
	}
	_ = col
	return Schema{Version: sch.Version + 1, Columns: cols}, nil
}

// dropColumn 纯函数结构变换。
func dropColumn(sch Schema, name string) (Schema, error) {
	if _, ok := sch.Column(name); !ok {
		return sch, fmt.Errorf("%w: cannot drop unknown column %q", ErrColumnNotFound, name)
	}
	cols := make([]Column, 0, len(sch.Columns)-1)
	for _, c := range sch.Columns {
		if c.Name != name {
			cols = append(cols, c)
		}
	}
	return Schema{Version: sch.Version + 1, Columns: cols}, nil
}

// alterType 纯函数结构变换。
func alterType(sch Schema, name string, t DataType) (Schema, error) {
	col, ok := sch.Column(name)
	if !ok {
		return sch, fmt.Errorf("%w: cannot alter unknown column %q", ErrColumnNotFound, name)
	}
	if t == TypeInvalid {
		return sch, fmt.Errorf("%w: column %q cannot change to invalid type", ErrIncompatibleSchema, name)
	}
	if col.Type == t {
		return sch, nil // 幂等：无实际变化
	}
	if !canConvert(col.Type, t) {
		return sch, fmt.Errorf("%w: column %q cannot change type %s -> %s safely",
			ErrIncompatibleSchema, name, col.Type, t)
	}
	cols := make([]Column, len(sch.Columns))
	for i, c := range sch.Columns {
		if c.Name == name {
			c.Type = t
		}
		cols[i] = c
	}
	return Schema{Version: sch.Version + 1, Columns: cols}, nil
}

// canConvert 判断读取/演进时 from 类型能否安全当作 to 类型使用。
func canConvert(from, to DataType) bool {
	if from == to {
		return true
	}
	// 唯一支持的跨类型兼容：int64 提升为 float64。
	if from == TypeInt64 && to == TypeFloat64 {
		return true
	}
	return false
}
