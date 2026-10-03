# columnar-compaction-store-go

一个纯本地文件模拟的列式数据存储组件（单 Go 包，零第三方依赖），提供：

- **列式写入与读取**：数据按列组织落盘；读取时只对用到的列发起磁盘读取（字节级列裁剪），并把简单过滤条件下推到段内逐行求值。
- **小文件合并 + 一致快照读取**：多个小段合并为大段，清单原子交换；合并进行中的并发读者看到的永远是合并前或合并后的完整快照，绝不会看到半截结果；合并在"临时段已写、新段已落盘未可见、已可见旧文件未清"等各窗口崩溃后都能回到一致状态。
- **表结构演进**：新增列 / 删除列 / 改名 / 可兼容的类型变更（int64 → float64），新旧数据混读语义确定；不兼容变更被明确拒绝。
- **列生命周期身份**：每列有跨改名不变的稳定 ID；改名后历史数据连续可读，删除后同名重生是全新身份，旧字节在任何读取方式下都不会被当成新列值。
- **段级跳过（zone-map）**：每个段带每列 min/max/NULL 依据，带过滤条件的扫描可整段跳过不可能命中的段；依据缺失、CRC 损坏或与真实数据对不上时安全回退完整扫描。
- **不静默容错**：列缺失、类型对不上、编码损坏（魔数/CRC/长度/非法值）都会返回可用 `errors.Is` 区分原因的错误。

## 1. 目录与落盘布局

一个 `Store` 对应磁盘上的一个目录（一张表）：

```
<store-dir>/
├── manifest.json               # 清单：结构历史 + 存活段列表（原子替换）
├── seg-0000000001.ccs          # 段文件（列式数据，不可变）
├── seg-0000000002.ccs
└── .seg-0000000003.ccs.tmp     # 正在写入/合并、尚未提交的临时段（崩溃即清理）
```

### manifest.json

```json
{
  "version": 7,                    // 清单单调版本，每次刷盘/合并/演进 +1
  "schema_history": [
    {"version":1,"columns":[{"ID":1,"Name":"id","Type":1,"Nullable":false}, ...]},
    {"version":2,"columns":[...]}  // 列带稳定 ID；改名沿用同一 ID，重生分配新 ID
  ],
  "segments": [
    {"id": 2, "file_name": "seg-0000000002.ccs", "row_count": 100, "schema_version": 1,
     "stats": {"id": {"type":1,"null_count":0,"int_min":1,"int_max":100,"crc":362437}}
    }
  ],
  "next_segment_id": 3,
  "next_column_id": 5              // 列稳定身份的分配水位（只增不减）
}
```

- 列定义带 `ID`（生命周期身份），段的 `stats` 是段级 min/max/NULL 依据
  （带 CRC，用于带过滤扫描时整段跳过，详见第 8 节）。

- 清单以 `manifest.json.tmp` 写入 → `fsync` → `rename` 原子替换 + 目录 `fsync`，
  任何时刻磁盘上只有旧版或新版，不存在中间态。
- `schema_history` 保留所有历史结构版本；每个段记录它写入时的 `schema_version`，
  读取时先按旧结构解码，再映射到当前结构。

### 段文件二进制格式（大端序）

```
固定头 16 字节:
  magic[4]="CCS1" | formatVersion u16 | schemaVersion u32 | rowCount u32 | nCols u16
列目录（nCols 项，让读取方无需触碰未用列）:
  nameLen u8 | name bytes | type u8 | frameLen u32
列帧（顺序与目录一致）:
  payload:
    nameLen u16 | name bytes      # 帧内自带身份，纳入 CRC，与目录交叉校验
    type u8 | count u32
    nullBitmap ceil(count/8) 字节  # 置 1 表示非 NULL
    values 按类型紧密排列           # int64/float64: 定长 8B；bool: 1B；string: u32 长度 + 字节
  crc u32                          # crc32-IEEE(payload)
```

- 读取列裁剪：读取方先读固定头 + 列目录（很小），按目录里的偏移只对投影列和过滤列
  发起 `ReadAt`，未用列的帧一个字节都不会从磁盘读取（有测试用计数 ReaderAt 证明）。
- 完整性：魔数、格式版本、目录长度、帧长度、文件长度、列名/类型/行数交叉校验、
  每帧 CRC32；任何损坏或截断都返回包装了 `ErrCorruptData` 的错误。

## 2. 写入与读取路径

- `Append(ctx, row)`：按**当前结构**校验后进入内存缓冲（行被规范化为只含声明列）。
  - 未声明列 → `ErrColumnNotFound`；Go 类型与列类型不符 / 非空列给 NULL → `ErrTypeMismatch`。
  - 缓冲达到 `FlushThreshold` 自动落段；也可显式 `Flush`。
- 落段：编码为列式字节 → 写 `.tmp` → `fsync` → `rename` 成正式段 → 原子更新清单。
- `Scan(ctx, ScanOptions{Columns, Filters})` 返回快照 `Scanner`：
  - 打开瞬间冻结段清单、当前结构与内存缓冲，并登记一个读引用；
  - 段按写入先后顺序读取，内存缓冲（最新数据）最后读取，因此行序等于写入序；
  - `Columns` 为空读全部列；`Filters` 为 AND 语义，支持 `== != < <= > >=`，
    NULL 对任何比较都不为真；过滤列即使未投影也会被加载；
  - `Next()` 耗尽时返回 `io.EOF`；`Close()` 释放快照引用。

## 3. 合并策略与一致读取

选段规则（`pickCompactionSegments`）：

1. 存活段数 ≥ `CompactionMinSegments`：合并**全部**段（全量合并，语义最简单、
   结果最确定）；
2. 否则当 `CompactionTargetRows > 0` 且存在 ≥2 个行数低于目标的小段：合并这些小段；
3. 其余情况（不足 2 段）不动作。

合并流程（`Compact`）：

1. 先刷盘内存缓冲；选出待合并段。
2. 把旧段按**当前结构**读出（自动应用类型提升），编码写入一个全新段
   `.tmp → rename`（此期间没有任何读者知道新段）。
3. 在互斥锁内一次性把清单的存活段集合替换为新段，并原子持久化清单。
4. 等待所有持旧快照的扫描器 `Close`（WaitGroup 计数）后，才物理删除旧段文件。

由此得到的保证：

- **快照一致性**：扫描器在读锁内取快照，清单交换对它要么已发生要么未发生；
  旧段文件在最后一个旧扫描器退出前不会被删除，因此读者永远读不到"合并到一半"
  或已被删除的文件（并发测试用每段 1 行、40 段、12 个持有快照的读者反复验证）。
- **崩溃恢复覆盖的时间窗口**（重启时 `cleanCrashArtifacts` 依据"清单只引用谁"
  做唯一裁决，要么完整回到合并前，要么干净停在合并后）：
  1. **新段还在 `.tmp`（已 fsync、rename 前）崩溃**：`.tmp` 一律清理，旧清单
     与旧段原封不动，完整回到合并前（注入点 `compactFailTmpFsynced`）。
  2. **新段已 rename 成正式文件名、但清单尚未交换时崩溃**：磁盘上新旧段并存，
     但清单只引用旧段；新段作为"无主段"被清理，行数不增不减、不重复，
     完整回到合并前（注入点 `compactFailAfterDataPublish`）。
  3. **清单已切成可见、但旧段尚未删除干净时崩溃**：清单只引用新段，重启后旧段
     作为无主段清理，干净停在合并后（注入点 `compactFailAfterSwap`）。
  - 三个窗口里的并发读者：清单交换是唯一的可见性边界，且快照读者持有的旧段
    在其 Close 前不会被删，因此读者任何时刻只能看到合并前或合并后的完整快照，
    看不到 `.tmp`、无主新段或半截数据。
- **交换前优雅失败回滚**：步骤 1–2 任何失败 → 删除新临时段，内存与磁盘清单保持
  旧状态，旧段原封不动，重试合并可成功（故障注入点 `compactFailBeforeSwap` 覆盖）。

## 4. 表结构演进与列生命周期身份

每个列在首次出现时获得一个单调分配的稳定 **ID**（清单的 `next_column_id`
水位只增不减）。段文件物理上按"写入时列名"存帧，读取时先做**身份绑定**
（`resolveFrameBindings`）：当前列带非零 ID 时，在段写入时结构里找到同
名帧的 ID，再用该 ID 定位当前列；ID 为 0 的旧存储回退为按名字绑定。

| 变更 | 是否允许 | 读写语义 |
| --- | --- | --- |
| **列改名** `RenameColumn(old,new)` | 允许（新名未被占用） | **沿用同一 ID**：旧段里旧名字节经 ID 绑定到新名字，改名前后历史数据连续可读；改名后只能用新名字写入/查询；改成自己是幂等 no-op |
| **同名重生**（先 `DropColumn(name)` 再 `AddColumn(同名)`） | 允许 | **分配全新 ID**：旧段同名字节属于已删除的旧身份，永不按新列解释——旧行在新列读出 NULL；新旧类型即使完全不兼容（int64→string）也只是这一列为空，**整表照常可读**；带列裁剪与不带列裁剪结果一致；旧值绝不可能以任何方式错位读回 |
| 新增列 | 仅允许 **Nullable** 列（分配新 ID） | 旧行该列读出为 NULL；非空新列会让旧数据无法填值，直接拒绝 |
| 删除列 | 允许 | 旧段里的物理帧与依据保留，但身份不再绑定，永不加载、永不出现在结果行 |
| int64 → float64 | 允许 | 读旧段时把 int64 无损提升为 float64；合并后以 float64 重写 |
| 同类型改同类型 | 幂等 | 不产生新版本 |
| 其它跨类型变更（string↔int64、float64→int64、bool↔* 等） | **拒绝** | 返回 `ErrIncompatibleSchema`，绝不按字节错位解释 |

- 演进入口：`AddColumn` / `DropColumn` / `RenameColumn` / `AlterColumnType`；
  演进前会强制刷盘，保证"哪一行按哪个结构写入"的边界落在段边界上。
- 段读取时的两条关键身份规则：
  - 存储类型无法安全转换到**绑定到的当前列**类型时，返回 `ErrIncompatibleSchema`；
  - 同名重生列的旧帧**绑定失败**（找不到同 ID 的当前列），因此不进入类型检查、
    不报错、读成 NULL——类型再不兼容也不会拖垮整张表。
- 段级跳过依据（第 8 节）同样按身份解析：改名后旧段依据经 ID 映射继续有效；
  重生列在旧段里映射不到依据，宁可整段不跳，也不会拿旧列极值错误排除新列数据。

## 5. 可断言区分的错误

```go
errors.Is(err, ErrColumnNotFound)      // 列不存在 / 行里出现未声明列
errors.Is(err, ErrTypeMismatch)        // 值类型不符 / 非空列 NULL / 过滤值类型不符
errors.Is(err, ErrCorruptData)         // 段文件魔数/CRC/长度/编码损坏或段文件缺失
errors.Is(err, ErrIncompatibleSchema)  // 不兼容的结构变更或存储类型无法演进
errors.Is(err, ErrManifestCorrupt)     // 清单 JSON 损坏或内容不自洽
errors.Is(err, ErrNotFound)            // 目录未初始化
errors.Is(err, ErrClosed)              // Store/Scanner 已关闭
```

扫描结束：`errors.Is(err, io.EOF)`。

## 8. 段级跳过（zone-map pruning）与安全回退

数据量上来后，仅靠列裁剪仍要触碰每个段。本组件为每个段、每一列在段
生成时计算一份极值依据，随清单**原子**持久化：

```json
"stats": {
  "age": {"type":1, "null_count":0, "int_min":10, "int_max":19, "crc":362437}
}
```

- 依据含：存储类型、NULL 计数、非 NULL 值的 min/max（int64/float64/string/bool
  各自用强类型字段，JSON 往返精度无损），以及对除 CRC 外字段的 `crc32` 校验。
- 合并时对合并后的数据**重新计算**依据，因此依据永远跟着数据保持一致。

### 跳过判定

带过滤条件的扫描在打开段文件**之前**，对每个段、每个过滤条件（AND 语义）：

- 段内该列全 NULL → 该比较不可能为真；
- `==`：值落在 `[min,max]` 之外 → 整段跳过；
- `< / <= / > / >=`：用 min/max 证明不存在满足方向的值 → 整段跳过；
- `!=`、bool 的大小比较等无法仅凭极值排除的情况 → 不跳过；
- 多个过滤必须**全部**证明不命中才跳过；任何一个不确定就读整段。

被跳过的段**一个字节都不从磁盘读取**；未跳过的段上列裁剪照常生效。
int64 段被演进为 float64 读取时，极值按 float64 口径比较。改名列经稳定
ID 映射回旧名依据；同名重生列在旧段映射不到依据，不参与跳过。

### 回退策略（宁可多读，绝不少行）

1. **依据缺失**：旧段没有 `stats`、该列无条目、或身份无法绑定 → 不跳过；
2. **依据损坏**：CRC 不自洽、NULL 计数越界等 → 在跳过决策时即判为不可信，
   不跳过（无需读段就能保证不会因损坏依据而漏行）；
3. **依据与数据对不上**：读出真实段后，用过滤列（必被整列加载）的真实值
   **重算** min/max/NULL 计数与清单依据逐列精确比对；一旦不符（哪怕 CRC
   形式完好但内容造假），立即丢弃本轮（可能已跳过若干段的）结果，自动用
   **完整扫描重跑一次**，最终结果与不跳过时逐行一致。

扫描结束后可通过扫描器的 `PruningStats()` 看到 `considered/skipped` 段数。

## 9. 本地验证

```bash
go build ./...          # 编译
go vet ./...            # 静态检查
gofmt -l .              # 格式检查（应无输出）
go test ./... -v        # 全部用例，日志按"[业务=...] 用例=... | 输入=... | 结论=..."打印
go test ./... -race     # 并发读写/合并竞态检测
go test ./... -cover    # 语句覆盖率（当前约 80%+）
```

测试覆盖的业务域：

- **列式写入-输入校验**：四类类型不符、非空 NULL、未声明列；
- **列式读取-裁剪与过滤下推**：全列/投影、六种比较操作符、AND、NULL 语义、
  以及"字节级列裁剪"（计数 ReaderAt 断言未投影列零读取）；
- **小文件合并**：选段阈值、按目标行数选小段、合并后行数/顺序不变、旧段清理、
  交换前失败回滚 + 重试成功、关闭重开数据不丢；
- **合并崩溃恢复**：`.tmp` 已 fsync 未 rename、新段已落盘但清单未交换、
  清单已交换但旧段未清理三个窗口的崩溃与重开恢复（完整回到合并前 / 干净停在
  合并后，行数不重不丢），以及窗口期间并发读者只见完整快照；
- **列生命周期身份**：改名后全列读/列裁剪/带过滤/合并/重开历史连续；同名重生
  （int64→string）旧数据读 NULL、整表可读、带不带裁剪一致、过滤不读旧字节；
  改名校验规则；段级跳过在改名后仍生效、重生后不滥用旧依据；
- **段级跳过**：该跳的整段跳（int/string/bool/全 NULL）、不该跳的不跳（`!=`、
  跨整域）、列裁剪与跳过组合、9 个阈值下跳过扫描与完整扫描结果逐一相等、
  合并后依据重算一致、依据缺失/CRC 损坏安全回退、依据造假被交叉校验识破并
  完整重扫描把漏掉的行捞回；
- **并发**：合并期间 12 个持快照读者 + 多轮 compact；4 写者 + 1 持续读者 + 周期 compact；
- **表结构演进**：新增/删除/类型变更规则、新旧段混读、跨段 int64→float64、
  合并前后语义稳定、不兼容变更拒绝、演进后旧类型写入拒绝；
- **损坏数据**：魔数、截断、翻字节导致 CRC 错、非法 bool 字节、损坏列在/不在
  投影中的不同表现、清单损坏三种形态、段文件丢失、未初始化目录、磁盘上类型不兼容。

## 10. 快速上手

```go
s, _ := columnar.Create("./data", columnar.Schema{Columns: []columnar.Column{
    {Name: "id", Type: columnar.TypeInt64, Nullable: false},
    {Name: "name", Type: columnar.TypeString, Nullable: true},
}}, columnar.DefaultOptions())
defer s.Close()

_ = s.Append(ctx, columnar.Row{"id": int64(1), "name": "alice"})

sc, _ := s.Scan(ctx, columnar.ScanOptions{
    Columns: []string{"name"},
    Filters: []columnar.Filter{{Column: "id", Op: columnar.OpGreaterEqual, Value: int64(1)}},
})
defer sc.Close()
for {
    row, err := sc.Next()
    if errors.Is(err, io.EOF) { break }
    // row == {"name": "alice"}
}

_ = s.AddColumn(ctx, columnar.Column{Name: "tag", Type: columnar.TypeString, Nullable: true})
_ = s.Compact(ctx)

// 改名：历史数据以新名字连续可读；删除后用同名 AddColumn 则是"同名重生"，
// 旧数据在新列读出 NULL（旧字节永不按新列解释）。
_ = s.RenameColumn(ctx, "name", "full_name")
```

## 设计边界（有意为之的简化）

- 单表/单目录、行全在内存缓冲、解码结果在扫描器内物化（演示组件，不做流式执行）。
- 合并为全量或小段合并，不做 Size-Tiered/Leveled 等复杂策略。
- 类型系统仅 int64/float64/string/bool；可兼容转换仅 int64→float64。
- 无后台线程：刷盘与合并由显式调用/阈值触发，行为完全确定、可测试。
