# columnar-compaction-store-go

一个零第三方依赖的**本地列式数据存储组件**（纯 Go，本地文件模拟落盘）。
覆盖四类能力：

1. **列式写入与读取**：数据按列块组织；读取只解码“投影列 + 过滤列”，并把
   简单谓词下推到行迭代；列缺失、类型不符、编码损坏都以**可区分的错误类别**
   显式返回，绝不静默跳过。
2. **小文件合并与一致读取**：多个小文件可合并成大文件；合并期间并发读者
   只会看到“合并前全集”或“合并后全集”，看不到中间态；合并中途失败自动
   回到合并前的一致状态，不留半截/孤儿数据。
3. **表结构演进**：新增列、删除列、重命名列、可兼容的类型变化；旧数据与新
   数据之间有确定稳定的读写语义；不兼容的类型变更被明确拒绝。
4. **可观测的测试**：并发读写/合并、结构演进、损坏与不兼容数据均有覆盖，
   拒绝原因可被 `errors.Is` 断言区分，`go test -v` 日志打印每个业务场景的
   输入与结论。

---

## 1. 目录与落盘布局

一个根目录 `dir` 可存放多张表，每张表一个子目录：

```
<dir>/
└── tables/
    └── <table>/
        ├── CURRENT                     # 文本指针：当前 manifest 文件名
        ├── MANIFEST-00000001           # 当前生效的完整元数据快照（JSON，磁盘上只保留 1 份）
        └── data/
            ├── data-0000001.ccol       # 一次 Append 写出一个数据文件
            ├── data-0000002.ccol
            └── data-000000N.ccol       # 合并产物（ID 单调递增）
```

清单文件名为 `MANIFEST-%08d`（无后缀），内容是 JSON。

### 1.1 MANIFEST（元数据快照）

每个 manifest 是一份**全量、不可变** JSON，包含：

- `id`：单调递增的 manifest 编号；
- `files`：当前可见数据文件列表（文件 ID、相对/绝对路径、写入时的
  `schemaVersion`、行数、创建时间）；
- `schemas`：**全部历史 schema 版本**（版本号从 1 连续递增）；
- `nextColID`：下一个可分配的列 ID（列 ID 永不复用）。

`CURRENT` 是 UTF-8 文本 `MANIFEST-<id>\n`。manifest 与 CURRENT 都通过
“**同目录临时文件 → fsync → 原子 rename**”写出，因此读取端要么读到旧的完整
快照、要么读到新的完整快照，不存在撕裂的中间态。旧 MANIFEST 在新快照换入
成功后被回收（读取器构造时已把快照解析进内存，不长期依赖清单文件）。

### 1.2 数据文件 .ccol

全部整数小端编码，布局：

```
偏移 0  : magic "CCSTDATA" (8B)
          u32 formatVersion(=1) | u32 schemaVersion | u32 numRows
          每个列块（按列 ID 升序）：
            u64 columnID | u32 valueType | u32 encLen | encLen 字节编码块
footer  : u32 chunkCount
          每条索引：u64 columnID | u32 type | u64 blockOffset | u32 encLen
          u64 footerOffset
          magic "CCSTDATA" (8B)
```

- 打开文件时只读头部和尾部 footer，建立 `columnID -> (offset,len,type)`
  索引；读取某列时按偏移直接取块，**不需要扫描别的列**，实现列裁剪。
- 文件头尾双魔数 + `footerOffset` 回指自洽校验，可识别截断/错位。

### 1.3 列块编码

```
外层 := u32(bodyLen) | body | u32(crc32c(body))          // Castagnoli
body := u32(nullBitmapLen) | nullBitmap | payload
位图 := 第 r 行位于 byte(r/8) 的 bit(r%8)，置 1 表示 NULL；填充位必须为 0
payload（只编码非 NULL 行，保持行顺序）：
  int64   8 字节小端
  float64 8 字节小端 IEEE-754
  bool    1 字节，仅允许 0/1
  string  uvarint 长度 + UTF-8 字节
```

外层 CRC、块长度、位图填充位、payload 是否恰好耗尽、bool 字节是否合法等
都会被校验；任何不一致统一归类为 `ErrCorruptData`。

---

## 2. 合并策略（Compaction）

`Compact()` 把**当前 manifest 中的全部数据文件**合并成一个新文件：

1. 在元数据串行锁下选定输入文件（文件数 < 2 时直接返回，无需合并）。
2. 按**当前 schema 视角**逐文件读列：
   - 列按 **ID** 对齐；文件写入时该列还不存在 → 该文件区间补全 NULL；
   - 文件中的旧类型与当前类型不同 → 按结构兼容规则**即时无损转换**；
   - 数据块缺失 / 类型不自洽 / CRC 损坏 → 整个合并失败，**不丢任何行**。
3. 以临时文件 + 原子 rename 写出**新数据文件**。此时 manifest 尚未引用它，
   任何读者都看不到它。
4. **换入窗口**（保证一致读的关键）：
   - 标记 `swapInProgress`，**等待所有在途读取结束**（引用计数归零），期间
     新开的读取在条件变量上等待；
   - 在窗口内原子保存并发布新 manifest（新文件集合恰好替换旧集合）；
   - 退出窗口，放行等待的读取。
5. 换入成功后，才**物理删除旧数据文件**——此刻已没有任何读者还能引用它们。

### 失败回滚

- 新文件写出后、换入前提供测试钩子 `setFailBeforeSwap(true)` 强制失败；
- 失败路径：不修改 CURRENT/manifest，删除新写出的半成品文件，退出换入窗口
  放行读者，旧文件原样保留，随后可再次正常合并；
- 因为数据文件本身是原子 rename 落盘的，任何进程崩溃都只会留下“旧文件”或
  “完整新文件”，不会留下半个文件。

### 并发模型

| 操作 | 互斥关系 |
|---|---|
| Append / AddColumn / DropColumn / Rename / ChangeType / Compact | 由一把元数据锁串行化“推进点” |
| Reader 构造与迭代 | 不持元数据锁；构造时对当前 manifest 加**快照引用计数** |
| 合并换入 | 等待引用计数归零并暂停新读取开始，保证换入是原子可见点 |

因此读者看到的文件集合永远是**某一次提交的完整快照**，行不会重复、丢失或
跨文件错位。

---

## 3. 表结构演进与兼容规则

列由 **(ID, Name, Type)** 描述：ID 一经分配**永不复用**；删除列只是从当前
版本摘除，旧文件与历史 schema 仍然保留；重命名只改名字，ID/类型/位置不变，
旧数据按 ID 在新名字下读出。

| 变更 | 是否允许 | 旧数据读语义 |
|---|---|---|
| 新增列 | 允许 | 旧文件没有该列块 → 旧行该列读为 **NULL**，新行正常 |
| 删除列 | 允许 | 当前 schema/投影中不可见；历史版本仍可解读旧文件 |
| 重命名列 | 允许 | ID 不变，旧数据在新列名下读出；旧名读取报 `ErrColumnNotFound` |
| `bool  -> string` | 允许（无损） | 即时转换为 `"true"` / `"false"` |
| `int64 -> string` | 允许（无损可逆） | 即时转换为十进制文本 |
| `int64 -> float64` | **拒绝** | 超过 2^53 的整数会丢精度，非安全转换 |
| `string -> int64/float64/bool` | **拒绝** | 解析可能失败/越界，语义不确定 |
| `float64 -> int64`、`bool -> 数值` 等 | **拒绝** | 无确定无损映射 |
| 改成相同类型 | 允许（空操作） | 不产生新版本 |

被拒绝的变更返回包装了 **`ErrIncompatibleChange`** 的错误，不会产生新
schema 版本。类型转换发生在**读取期**（无需重写旧文件）；转换函数本身只
接受上表中的安全组合，理论上不会失败，一旦出现无法转换则以
`ErrTypeMismatch` 暴露而不是静默错位。

### 读取时的损坏 vs 演进 区分

读取某列时先用**文件写入时的 schema 版本**核对块类型：

- 块类型 ≠ 该历史版本记录的类型 → 磁盘被篡改，报 `ErrCorruptData`；
- 块类型 == 历史类型，但 ≠ 当前类型 → 正常演进，按兼容规则转换。

这保证“坏数据”不会被伪装成“类型演进”而静默吞掉。

---

## 4. 错误分类（拒绝原因可断言）

所有错误都包装为统一的 `columnar.Error`，并可通过 `errors.Is` 区分：

| 哨兵 | 触发场景 |
|---|---|
| `ErrColumnNotFound` | 投影/过滤列不存在、写入批次缺列或多列、删除不存在的列 |
| `ErrTypeMismatch` | 批次列类型与 schema 不符、过滤字面量类型不符、列切片长度不自洽、不可转换 |
| `ErrCorruptData` | 魔数错、CRC 不匹配、文件截断、类型码非法、footer 不自洽、manifest JSON 坏、CURRENT 悬空 |
| `ErrIncompatibleChange` | 不安全的列类型变更被拒绝 |
| `ErrInvalidArgument` | 空批次、空表名、重复投影列等调用方错误 |

错误字符串还带操作名（如 `datafile.decode`）、文件路径、列名与细节，便于
定位。

---

## 5. 本地验证

无需任何外部服务或第三方依赖，Go 1.25+ 即可。

```bash
# 普通全量测试（含 Example）
go test ./... -v

# 竞态检测 + 多次重复（强烈建议，用于验证并发读/写/合并）
go test ./... -race -count=1 -v

# 只看某类业务场景
go test ./... -v -run 'TestCompact'            # 合并 / 并发一致 / 失败回滚
go test ./... -v -run 'TestSchemaEvolution'    # 结构演进与不兼容拒绝
go test ./... -v -run 'TestCorrupt'            # 损坏与不兼容数据
go test ./... -v -run 'TestConcurrent'         # 写/读/合并交错压力
go test ./... -v -run Example                  # 端到端使用示例
```

`go test -v` 的日志按“业务场景 → 输入 → 结论”输出，例如：

```
[场景1 新增列] 输入: 旧数据 id=[10,20], flag=[true,false]；执行 AddColumn(score,int64)
[场景1 新增列] 结论: 旧两行 score=NULL,NULL；新行 score=99（旧行 NULL 语义稳定）
[场景4 拒绝不兼容变更] 输入: int64->float64；结论: 明确拒绝（大整数可能丢精度）
[场景 并发读+合并] 结论: 16 个读者全部读到完整 24 行快照，无重复/缺失/半截结果
```

### 最小用法

```go
t, _ := columnar.Open("/tmp/mystore", "orders")
t.AddColumn("id", columnar.TypeInt64)
t.AddColumn("city", columnar.TypeString)

t.Append(&columnar.Batch{Columns: []*columnar.Column{ /* 列等长 */ }})

// 列裁剪 + 过滤下推：只解码 id 与 city，结果只返回 id
r, _ := t.Reader([]string{"id"}, &columnar.Filter{Predicates: []columnar.Predicate{
    {Column: "city", Op: columnar.OpEq, Literal: columnar.Value{
        Type: columnar.TypeString, Str: "BJ", Valid: true}},
}})
defer r.Close()
for {
    row, ok, err := r.Next() // err 可能是 ErrCorruptData / ErrTypeMismatch …
    if err != nil { /* errors.Is 区分处理 */ }
    if !ok { break }
    _ = row.Row()["id"]
}

stats, _ := t.Compact() // 小文件合并
```

---

## 6. 代码结构

| 文件 | 职责 |
|---|---|
| `types.go` | `ValueType/Value/Column/Batch`，列式内存表示与自检 |
| `schema.go` | schema 版本历史、列 ID 分配、演进规则、无损类型转换 |
| `encoding.go` | 列块编解码、空值位图、CRC32C、损坏判定 |
| `datafile.go` | `.ccol` 文件原子写入、footer 索引、按列读取 |
| `manifest.go` | manifest 快照 JSON、CURRENT 原子指针、旧清单回收 |
| `filter.go` | 可下推谓词（=,<> <,<=,>,>=,IS [NOT] NULL）与三值逻辑 |
| `store.go` | Table：打开/写入/快照读取/DDL、引用计数与换入窗口 |
| `compaction.go` | 合并、失败回滚、多文件列式拼接 |
| `errors.go` | 统一错误类型与可断言的错误分类 |

测试：`smoke_test.go`、`schema_evolution_test.go`、`compaction_test.go`、
`concurrency_test.go`、`corruption_test.go`、`example_test.go`。
