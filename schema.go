package columnar

import (
	"context"
	"fmt"
)

// 结构演进的确定语义：
//   - 新增列：旧行在该列读出为 NULL；新列必须 Nullable（旧数据没有值）。
//   - 删除列：列定义从当前结构移除；旧段里的物理字节保留但永不对外可见。
//   - 列改名：列的生命周期 ID 不变，仅当前结构里的名字变化；旧段按写入时
//     结构解析出 ID 再映射到新名字，因此改名前后历史数据连续可读。
//   - 同名重生：删除列后再以同名新增，会分配全新的生命周期 ID；旧段里的
//     老列字节属于旧 ID，对新列一律读出 NULL，任何读取方式下都不会把老值
//     错位当成新值（即使新旧类型不兼容，也只影响该列，绝不让整表不可读）。
//   - 类型变更：当前仅允许 int64 -> float64（精确无损地转成 float64，
//     |x| < 2^53 时值严格相同），其余跨类型变更一律拒绝。
// 每次演进都会产生一个新版本并追加到 SchemaHistory，旧段按写入时版本解码后
// 统一 evolveRow 到当前结构，因此新旧数据混读语义稳定。

// AddColumn 新增列（旧数据该列读出为 NULL）。新列分配全新的生命周期 ID，
// 因此与历史上同名但已删除的列没有任何关系（同名重生场景）。
func (s *Store) AddColumn(ctx context.Context, c Column) error {
	if err := s.checkOpen(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flushLocked(ctx); err != nil {
		return err
	}
	c.ID = s.m.NextColumnID // 生命周期 ID 由存储统一分配，忽略调用方传入值
	if c.ID == 0 {
		c.ID = 1
		for _, sch := range s.m.SchemaHistory {
			for _, col := range sch.Columns {
				if col.ID >= c.ID {
					c.ID = col.ID + 1
				}
			}
		}
	}
	next, err := addColumn(*s.m.currentSchema(), c)
	if err != nil {
		return err
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
		return nil // 幂等：类型未变，不产生新版本
	}
	return s.commitSchemaLocked(next)
}

// RenameColumn 列改名：生命周期 ID 不变，历史数据在新名字下连续可读。
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
		return nil // 幂等：名字未变，不产生新版本
	}
	return s.commitSchemaLocked(next)
}

// renameColumn 纯函数结构变换：只改名字，ID/类型/可空性全部保留。
func renameColumn(sch Schema, oldName, newName string) (Schema, error) {
	if newName == "" {
		return sch, fmt.Errorf("%w: column name must not be empty", ErrIncompatibleSchema)
	}
	if _, ok := sch.Column(oldName); !ok {
		return sch, fmt.Errorf("%w: cannot rename unknown column %q", ErrColumnNotFound, oldName)
	}
	if oldName == newName {
		return sch, nil // 幂等：无实际变化
	}
	if _, exists := sch.Column(newName); exists {
		return sch, fmt.Errorf("%w: column %q already exists", ErrIncompatibleSchema, newName)
	}
	cols := make([]Column, len(sch.Columns))
	for i, c := range sch.Columns {
		if c.Name == oldName {
			c.Name = newName
		}
		cols[i] = c
	}
	return Schema{Version: sch.Version + 1, Columns: cols}, nil
}

// addColumn 纯函数结构变换。
func addColumn(sch Schema, c Column) (Schema, error) {
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
	cols := make([]Column, 0, len(sch.Columns)+1)
	cols = append(cols, sch.Columns...)
	cols = append(cols, c)
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
