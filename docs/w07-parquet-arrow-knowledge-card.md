# W07 知识卡片：Parquet、Arrow 与数据布局

> Albert · W7：Parquet、Arrow、行/列式、压缩、Row Group、Batch。**技术 L3 通过（2026-11-18 20:10，Asia/Shanghai）**；**PR 待提交，未合并**。

## 核心概念

- **行式 vs 列式**：行式将一条记录的字段邻近组织，适合整行访问；列式把同列数据组织在一起，便于选列、编码与向量化。CSV 是按记录排列的文本交换格式，不等同于数据库行存储引擎。
- **Parquet 是持久化列式文件格式**：文件由 Row Group 组成；一个 Row Group 包含各列的 Column Chunk，列块由 Page 组成。编码/压缩主要在页级处理；统计信息可帮助跳过不相关数据，但本轮未测谓词过滤。
- **Arrow 是列式内存格式**：用类型化 buffer 表示值、有效位图，以及变长数据的 offsets/data 等；支持跨语言内存互操作。不是“Arrow 只存在内存里”。
- **Arrow IPC 是序列化/交换协议**：有 file 与 stream 形式；本轮测的是 IPC file，经 `OSFile + open_file().read_all()` 读成 Table，非 mmap，不是 stream，也不能直接等同于零拷贝。
- **Row Group ≠ RecordBatch ≠ Table**：Row Group 是 Parquet 文件布局/读取划分；RecordBatch 是一批等行数列数组；Table 的列可由多个 chunk 构成。三个层次的边界不要求相同。流式按批消费可以限制在途数据，但本轮终点是持有完整结果 Table，没测流式峰值。
- **编码 ≠ 压缩**：字典、RLE 等编码利用数据规律；ZSTD 等 codec 压缩编码后的数据。Parquet `compression="NONE"` 不等于关闭编码；本轮仍 `use_dictionary=True`。
- **文件大小 ≠ Table.nbytes ≠ RSS**：前者是序列化字节；Table.nbytes 是逻辑 buffer 字节；RSS 包括运行时、表、临时分配、共享/父缓冲及分配器驻留等。ΔRSS 是读后减读前，不是表大小。

## 真实实验：条件与结果

用户 JSON：WSL2，Python 3.10.12、PyArrow 25.0.1、psutil 7.2.2；100 万行、seed=42，6 列（4 个 int64、2 个 string）。单线程，非 mmap；每次独立 worker，父进程先按字节读目标文件到 EOF 预热；每配置 5 次。已知惰性模块移至基线/计时前，终点为 `pa.Table`，不含摘要或 equals。以下内存均为 **ΔRSS 中位数**，不是总 RSS；MiB=2²⁰ bytes。

### A：全列读取（v2）

| 格式 / 配置 | 文件 MiB | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|---:|
| CSV | 45.4611 | 117.7 | 140.1 | 78.46 | 49.59 |
| Parquet NONE | 33.1399 | 57.2 | 182.8 | 108.49 | 50.07 |
| Arrow IPC | 49.5962 | 11.8 | 15.7 | 54.05 | 49.59 |

### B：只读 amount_cents（原文件不变）

| 格式 / 配置 | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|
| CSV | 68.8 | 97.2 | 26.18 | 7.63 |
| Parquet NONE | 6.6 | 113.8 | 22.42 | 7.75 |
| Arrow IPC | 6.7 | 23.3 | 46.04 | 7.63 |

全列/单列中位耗时比：CSV **1.71×**，Parquet **8.70×**，IPC **1.77×**。Parquet 6.6 ms 与 IPC 6.7 ms 不足以给出稳定排名。

投影是在读 API 中完成：CSV `include_columns`（仍扫描整份文本）、Parquet `columns`、IPC `IpcReadOptions(included_fields=[2])`。不是先全表读完再 select，也没测真实 IO 字节减少量。

### C：同源、同参数重新写成对 Parquet

| 格式 / 配置 | 文件 MiB | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|---:|
| Parquet NONE（本轮新写） | 33.1399 | 44.796 | 46.061 | 108.60 | 50.07 |
| Parquet ZSTD level 3（本轮新写） | 15.7470 | 74.510 | 143.711 | 111.07 | 50.07 |

两份均从同一 `orders.arrow` 源表新写，编码/列序与 10×10 万行组相同，只改 NONE ↔ ZSTD level 3 压缩配置；**不能拿跨批次 warm-02 当压缩基线**。省 **52.48%** 文件空间，热读中位数增加 **66.3%**；ΔRSS 相差 **2.46 MiB**，不视为显著差异。写入、源表加载不计入读取时间。

三项均有摘要与独立分批 `equals` 通过记录；B 只验证投影列，C 与源表比较。金额合计 **50,039,843,155 分**。这是数据一致性验证，不是 Albert 的 L3 能力验收。

## 证据支持的取舍

| 工作负载 | 优先考虑 | 不要忘记 |
|---|---|---|
| 可读、兼容性优先的交换/导入 | CSV | 文本扫描与转换成本；本轮投影仍需扫整份文本 |
| 数据湖、离线选列分析、空间敏感 | Parquet | NONE 仍编码；ZSTD 更小但本轮热读更慢 |
| Arrow 生态中间交换、本地快速加载 | Arrow / IPC | 本轮全列热读快，但文件大；零拷贝取决于具体路径 |

## 易错点与边界

1. **“三格式只改变布局。”** 错。格式、编码、解析器与 PyArrow 实现共同变化，只能比较这组组合。
2. **“压缩小，所以必然读得快且更省内存。”** 错。热缓存中可能增加解压工作；本轮只测总墙钟，没做 CPU 分段证实，不能把全部增幅归因解压。
3. **“单列 Table 7.63 MiB，所以 IPC 只占 7.63 MiB。”** 错。实测 ΔRSS 46.04 MiB；共享父缓冲保留是候选解释，用户进程未确认，不当作根因定论。
4. **“v2 优化了 Parquet 读取。”** 错。v1 计时混入 dataset 等惰性导入，v2 修正计时与 peak 窗口；Parquet median 54.1→57.2 ms 未变快，max 1539.3→182.8 ms 只是该批次观察。baseline 升高，ΔRSS 减少不代表实现更省内存。
5. **“热读说明冷盘/对象存储也一样。”** 错。未测冷读、实际 IO 请求/磁盘字节、CPU 分解；不外推网络/冷存储收益。
6. **“max 就是 p99，采样 peak 就是真峰值。”** 错。只有 5 次；约 2 ms 采样可能漏尖峰，所有慢轮保留，不臆断首轮慢的根因。

## 技术验收与交付状态

- **初测 L2 → 补强后技术 L3 通过**（2026-11-18 20:10，Asia/Shanghai）：初测欠缺诊断因果；补强自测中能提出“IO 节省超过解压成本”的假设，控制同一数据与其余条件、重复测试，提出采集总耗时、实际磁盘读取量及 IO 耗时、进程 CPU 时间，以“读盘更少、CPU 时间更多、总耗时更短”的组合支持假设。
- 这是诊断设计与证据判据，不是新增实测；归档仍未测 IO/CPU 分解，全部实验数字不变。
- “仅大小和总耗时不足以拆分因果”是后续讲解补充，不归为用户独立作答。
- **仓库交付：已获授权，PR 待提交，未合并**。技术 L3 与仓库交付分开记录；实验细节见[完整报告](w07-data-formats-comparison-report.md)，运行与目录映射见[README](../data-formats/w07/README.md)。

### 来源

均为用户提供的原始 JSON，已按原字节只读归档；数字从 `summary/files` 提取，并用 `trials` 复核统计。原附件名与 JSON SHA-256 见[完整报告](w07-data-formats-comparison-report.md)。下列逻辑 `results/` 均在仓库 `benchmarks/w07-data-formats/` 下：

- [S1] [`results/warm-01/summary.json`](../benchmarks/w07-data-formats/results/warm-01/summary.json)：v1，仅用于测量口径复盘。
- [S2] [`results/warm-02/summary.json`](../benchmarks/w07-data-formats/results/warm-02/summary.json)：v2 全列。
- [S3] [`results/warm-03-col/summary.json`](../benchmarks/w07-data-formats/results/warm-03-col/summary.json)：单列投影。
- [S4] [`results/warm-04-zstd/summary.json`](../benchmarks/w07-data-formats/results/warm-04-zstd/summary.json)：成对压缩。

原始数据文件不在材料中，未独立重跑实验或 equals；沿用 JSON 内的校验记录，机制解释与实测结论分开。复现脚本与依赖：[生成器](../data-formats/w07/generate_data.py)、[全列 v2](../data-formats/w07/benchmark_read_v2.py)、[投影](../data-formats/w07/benchmark_projection.py)、[压缩](../data-formats/w07/benchmark_compression.py)、[requirements.txt](../data-formats/w07/requirements.txt)。

[S1]: ../benchmarks/w07-data-formats/results/warm-01/summary.json
[S2]: ../benchmarks/w07-data-formats/results/warm-02/summary.json
[S3]: ../benchmarks/w07-data-formats/results/warm-03-col/summary.json
[S4]: ../benchmarks/w07-data-formats/results/warm-04-zstd/summary.json
