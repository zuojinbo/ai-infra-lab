# W07 数据格式比较报告：CSV、Parquet 与 Arrow IPC

> Albert · 52 周 AI Infra 学习 W7。技术验收：**L3 通过（2026-11-18 20:10，Asia/Shanghai）**；仓库交付：**已获提交授权，PR 待提交，未合并**。技术能力验收与仓库交付状态分别记录。

## 1. 研究问题

- **实验 A（全列格式对照）**：相同订单数据读成完整 `pyarrow.Table`，CSV、Parquet NONE、Arrow IPC 的文件大小、热读耗时与驻留内存有何差异？
- **实验 B（投影对照）**：对每种格式，读取范围从全部 6 列变为仅 `amount_cents`，耗时和内存如何变化？
- **实验 C（压缩对照）**：同一源表、相同 Parquet 写入参数下，NONE 与 ZSTD level 3 如何权衡文件大小、热缓存全列读取时间及内存？

## 2. 实验假设

- **A**：CSV 的文本扫描和类型转换可能更贵；IPC 与 Arrow 内存表示接近，可能降低读取物化成本；Parquet 即使不压缩也可能因编码而较小。三者是**格式＋库实现＋编码路径的组合比较**，不能把差异只归因于行式/列式。
- **B**：少转换、少解码和少物化列通常有利；CSV 仍需扫描整份文本，收益未必等于列式格式。IPC 的读 API 投影不保证 RSS 按逻辑表大小同比下降。
- **C**：ZSTD 通常缩小文件；在热缓存下，其额外解压工作可能抵消字节减少的好处，读取未必更快，RSS 也未必更低。以实测为准，不预设结论。

## 3. 实验环境

以归档 JSON 的 `environment` / `limits` 为准。历史实验工作目录为 `/root/data-formats-lab`；该路径仅保留为环境记录，不是仓库复现依赖。

| 项目 | 记录 |
|---|---|
| 系统 | WSL2，Linux 6.6.87.2-microsoft-standard-WSL2，x86_64，glibc 2.35 |
| Python | 3.10.12，GCC 11.4.0 |
| 依赖 | PyArrow 25.0.1；psutil 7.2.2；v2、投影、压缩记录 pandas 未安装 |
| 可见逻辑 CPU | `os.cpu_count()=16`，不等于实验使用 16 线程 |
| 读取并行度 | `use_threads=False`；Arrow CPU pool=1、IO pool=1 |
| 映射与预取 | Parquet `memory_map=False, pre_buffer=False`；IPC 使用 `pa.OSFile`，非 mmap |
| 缓存 | 每个 worker 启动前，父进程按 8 MiB 块顺序读取目标文件到 EOF；依赖 OS 页缓存，不在预热时哈希 |
| 测量安排 | 每种格式/压缩配置各 5 次，每次独立 worker；seed=42 打乱每轮顺序；无删轮 |
| 内存采样 | 父进程目标间隔约 2 ms；调度可使实际间隔更长，短尖峰可能漏采 |
| 未记录的环境信息 | CPU 型号、物理内存容量、底层磁盘及宿主负载；不能补写为已知配置 |

这里的“热读”是协议定义，不是每个页面都常驻的硬保证。所有 worker 在 RSS 基线和计时前导入已定位的惰性模块：`pyarrow.dataset`、`pyarrow._dataset`、`pyarrow._dataset_parquet`、`pyarrow._dataset_parquet_encryption`、`pyarrow._dataset_orc`、`pyarrow.fs`，并触碰相应 FileFormat 对象；不提前打开数据文件或分配 Table。该处理只排除已知模块加载，不保证消除全部首次初始化。

## 4. 基线配置

### 数据与写入

- 同一份 1,000,000 行订单数据，生成 seed=42；按 100,000 行批次生成；金额为整数分、时间为 Unix 秒。
- Schema：`order_id:int64, user_id:int64, amount_cents:int64, city:string, event_time:int64, category:string`。本数据无空值；城市 8 类、品类 6 类。
- A/B 共用 `data/orders.csv`、`data/orders.parquet`、`data/orders.arrow`；CSV 带表头、UTF-8、无压缩；IPC 为无压缩 file 格式，不是 stream 格式。
- 原始 Parquet：`compression="NONE", row_group_size=100000, use_dictionary=True`。NONE 只关闭通用压缩，不关闭字典等编码；100 万行形成 10 个、每个 10 万行的 Row Group。
- A 基线为 CSV 全列读取；B 在每种格式内以 A 的全列结果为基线；C 以本轮新写的 `orders_none.parquet` 为基线，**不是拿 warm-02 的 Parquet 耗时做压缩基线**。

### 读取终点与 API

终点统一为持有 `pa.Table`，不含 pandas 转换或业务聚合。A 使用 `pacsv.read_csv`（显式全列 schema）、`pq.read_table`、`open_file(pa.OSFile).read_all()`。B 在读取 API 中选择列，不是全表读完再 `Table.select()`：

| 格式 | B 的选择方式 |
|---|---|
| CSV | `ConvertOptions(include_columns=['amount_cents'], column_types=显式全列schema)` |
| Parquet | `pq.read_table(..., columns=['amount_cents'])` |
| IPC | `IpcReadOptions(included_fields=[2], use_threads=False)` 传给 `open_file`，然后 `read_all()` |

C 从已有 `orders.arrow` 加载同一张源 Table，在新输出目录通过同一个写入函数写出两份 Parquet。写入和源表加载均不计入读耗时。两份共用的完整显式写入参数为：

```python
row_group_size=100000
use_dictionary=True
version="2.6"
write_statistics=True
use_byte_stream_split=False
column_encoding=None
data_page_version="1.0"
use_compliant_nested_type=True
store_schema=True
write_page_index=False
write_page_checksum=False
data_page_size=None
write_batch_size=None
dictionary_pagesize_limit=None
flavor=None
use_deprecated_int96_timestamps=None
coerce_timestamps=None
allow_truncated_timestamps=False
store_decimal_as_integer=False
```

仅压缩配置分别为 `compression="NONE", compression_level=None` 和 `compression="ZSTD", compression_level=3`。JSON 记录 ZSTD 可用、实际 level=3；两份均为 10×100,000 行组，元数据检查 schema 列序、行组布局和各列 encodings 一致，codec 分别为 UNCOMPRESSED/ZSTD。部分 `category` 列块的 `total_uncompressed_size` 相差 1 byte，JSON 已保留该差异；“写入参数/编码相同”不等于所有页元数据字节完全相同。

### 已完成实验的复现命令

从仓库根目录进入 `data-formats/w07`；环境创建见[实验 README](../data-formats/w07/README.md)。默认数据目录约定为脚本目录内的 `./data`（读取脚本需显式传 `--data-dir ./data`）。新结果使用 `./results/retest-*`，不覆盖历史归档。

```bash
cd data-formats/w07
source .venv/bin/activate

# 仅从零重建且 ./data 尚无目标文件时执行；已有数据请跳过
python generate_data.py --rows 1000000 --output-dir ./data --seed 42

# A：v2 全列
python benchmark_read_v2.py --data-dir ./data \
  --output-dir ./results/retest-02 --runs 5 --seed 42 --sample-interval-ms 2

# B：单列投影（显式指定，与默认值相同）
python benchmark_projection.py --data-dir ./data \
  --output-dir ./results/retest-03-col --runs 5 --seed 42 \
  --columns amount_cents --sample-interval-ms 2

# C：本轮重新写成对 Parquet 后做全列热读
python benchmark_compression.py --data-dir ./data \
  --output-dir ./results/retest-04-zstd --runs 5 --seed 42 --sample-interval-ms 2
```

生成器使用 `--rows/--output-dir/--seed`，不接受读取脚本的 `--data-dir/--runs`。已有目标文件时拒绝覆盖；再次复现应另选新目录，保留既有结果。A/B/C 输出均含 `summary.json`、`trials.csv`；C 另有派生 Parquet 和 `derived_manifest.json`。归档仅包含四份原始 `summary.json`，不包含生成数据、派生 Parquet、原实验 `trials.csv` 或 `derived_manifest.json`；各轮 trials 已内嵌于 summary JSON。

文中的历史逻辑路径 `results/warm-*` 均位于仓库的 **`benchmarks/w07-data-formats/`** 下，即 `benchmarks/w07-data-formats/results/warm-*/summary.json`；不是脚本当前工作目录里的新输出。历史记录仍保留原目录 `/root/data-formats-lab/results/warm-*`。具体映射与只读规则见[README](../data-formats/w07/README.md)。

脚本：[生成器](../data-formats/w07/generate_data.py)、[v1（历史测量口径）](../data-formats/w07/benchmark_read_v1.py)、[v2 全列](../data-formats/w07/benchmark_read_v2.py)、[投影](../data-formats/w07/benchmark_projection.py)、[压缩](../data-formats/w07/benchmark_compression.py)。依赖见 [requirements.txt](../data-formats/w07/requirements.txt)。

## 5. 唯一改变的变量

| 实验 | 本轮改变 | 保持不变 | 因果解释边界 |
|---|---|---|---|
| A 全列格式 | CSV / Parquet NONE / IPC 整套格式及对应 PyArrow 读取实现 | 逻辑内容、全列 Table 终点、热缓存协议、线程、重复次数 | 不是纯布局单变量；编码、解析器和实现随格式共同变化 |
| B 单列投影 | 每种格式内，读取列集合从全 6 列变为 `amount_cents` | 原文件、同口径测量协议、schema 类型、线程与缓存设置 | 比较 A 与 B 的批次中位数；不是同一时刻的严格成对随机 A/B，跨批次噪声仍存在 |
| C 压缩 | 同源新写的 NONE ↔ ZSTD level 3 预定压缩配置 | 同一源 Table、完整 shared write kwargs、列序、编码、10×10 万行组、全列读协议 | level 3 是压缩配置的一部分；不能用跨批次 warm-02 耗时替代本轮 NONE |

## 6. 采集指标

- 文件 bytes 及 MiB：`1 MiB = 1,048,576 bytes`。
- `elapsed_s`：`perf_counter` 覆盖读取并物化为 Table，含读路径内部工作；不含进程启动、模块预加载、父进程预热、写文件、摘要、equals 校验。报告 median 和 max；max 是 5 次样本最大值，不是 p99。
- `rss_baseline_bytes`：预加载与线程设置后、读取前的 RSS；`rss_end_bytes`：读完且仍持有 Table、摘要前的 RSS。
- **ΔRSS = end − baseline**。以下主结果表的“内存”均指 ΔRSS 中位数，不能简称为表大小或进程总内存。
- `sampled_peak_rss_bytes`：仅 GO→READ_DONE 读取窗口采样的**绝对 RSS** 最大值，摘要开始前结束；近似 2 ms 采样可能漏峰。`ru_maxrss` 是截至该时刻进程生命周期高水位，不能当作相对基线的读取峰值。
- `Table.nbytes`：表内 buffer 的逻辑字节，既不是文件大小，也不覆盖进程、临时对象、分配器保留及可能的共享父缓冲。
- 校验：每轮摘要，独立进程按 100,000 行对齐批次做 `table.equals`。未测实际 IO 请求字节、磁盘 IO 字节和 CPU 分解，不以文件大小/墙钟推算“真实 CPU 指标”。

## 7. 实验结果

### A：v2 全列格式对照 [S2]

| 格式 / 配置 | 文件 MiB | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|---:|
| CSV | 45.4611 | 117.7 | 140.1 | 78.46 | 49.59 |
| Parquet NONE | 33.1399 | 57.2 | 182.8 | 108.49 | 50.07 |
| Arrow IPC | 49.5962 | 11.8 | 15.7 | 54.05 | 49.59 |

### B：只读 `amount_cents` [S3]

原文件未改写，文件大小同 A；表中只列投影结果，不把单列 Table 大小冒充文件大小。

| 格式 / 配置 | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|
| CSV | 68.8 | 97.2 | 26.18 | 7.63 |
| Parquet NONE | 6.6 | 113.8 | 22.42 | 7.75 |
| Arrow IPC | 6.7 | 23.3 | 46.04 | 7.63 |

按未舍入的中位数计算，全列/单列耗时比：CSV **1.71×**、Parquet **8.70×**、IPC **1.77×**。Parquet 6.6 ms 与 IPC 6.7 ms 差异很小，结合样本数量和 max，不据此宣称稳定排名。

### C：同源新写 Parquet 压缩对照 [S4]

| 格式 / 配置 | 文件 MiB | 读取 median ms | 读取 max ms | ΔRSS median MiB | Table.nbytes MiB |
|---|---:|---:|---:|---:|---:|
| Parquet NONE（本轮新写） | 33.1399 | 44.796 | 46.061 | 108.60 | 50.07 |
| Parquet ZSTD level 3（本轮新写） | 15.7470 | 74.510 | 143.711 | 111.07 | 50.07 |

ZSTD 文件减少 **52.48%**；热读中位数增加 **66.3%**。计算分别为 `1 - 16511945/34749755` 与 `74.510042.../44.796281... - 1`。ΔRSS 中位数差 **2.46 MiB**，只有 5 次、未做显著性分析，不据此判定稳定内存差异；两者 Table 均为 50.07 MiB。

### RSS 口径展开（单位均为 MiB）

| 实验 | 格式 / 配置 | baseline median | end median | 读窗口 sampled peak median | sampled peak max |
|---|---|---:|---:|---:|---:|
| A 全列 | CSV | 64.30 | 142.75 | 142.75 | 142.88 |
| A 全列 | Parquet NONE | 64.41 | 172.90 | 172.90 | 173.15 |
| A 全列 | Arrow IPC | 64.29 | 118.35 | 118.35 | 118.47 |
| B 单列 | CSV | 64.55 | 90.73 | 90.73 | 92.56 |
| B 单列 | Parquet NONE | 64.67 | 87.09 | 87.09 | 87.34 |
| B 单列 | Arrow IPC | 64.67 | 110.70 | 110.70 | 110.71 |
| C 压缩 | Parquet NONE（本轮新写） | 65.69 | 174.29 | 174.29 | 174.41 |
| C 压缩 | Parquet ZSTD level 3（本轮新写） | 65.68 | 176.63 | 176.63 | 176.75 |

各列统计独立取中位数，`median(end) - median(baseline)` 未必等于 `median(end-baseline)`。本表 sampled peak 在这些汇总中与 end 接近或相同，不证明没有临时分配尖峰。

### 正确性与慢轮保留

- A/B 均 `digests_match=true`、`strong_equals.passed=true`；B 校验的是投影单列，不冒充全表 equals。C 两份新文件均与源表摘要、分批 equals 一致。
- 均为 1,000,000 行；`amount_cents` 的 int64 合计为 **50,039,843,155 分**。sum 是正确性证据之一，不能替代完整 equals。
- A 的各格式首次样本分别对应其 max（CSV 140.1、Parquet 182.8、IPC 15.7 ms）；B 亦保留各格式首轮慢值（97.2、113.8、23.3 ms）。C 的首个 ZSTD 样本为 143.711 ms，NONE 的 max 为 46.061 ms。未剔除首轮，也未把剩余慢值臆断为特定原因。

## 8. 原因分析

### A：格式与实现的共同取舍

CSV 要做文本边界识别、数值转换和字符串构建；IPC 布局与 Arrow 内存表示接近，物化转换较少，与本轮最快全列热读相符，但**本实验非 mmap，不能把它写成“零拷贝测量”**。Parquet 需要解码为 Arrow，其 NONE 文件依然比 CSV 小，说明“关闭压缩”并不等于没有编码；不能把全部大小差异定量归功于字典编码，因为未做编码开关对照。

### B：投影收益与逻辑/驻留内存分离

CSV 少做未选列转换，但仍扫描整份文本；Parquet 可只选所需列的列块解码；IPC 在 reader 层选择字段，而非事后 select。以上机制能解释投影方向，但未测 IO 请求量，不能声称实际磁盘读取量减少了某个比例。

IPC 单列 Table 只有 7.63 MiB，ΔRSS 仍为 46.04 MiB。可能涉及共享父缓冲的保留、读实现临时分配或分配器驻留；**“父缓冲保留”只是候选解释**。Parquet Table.nbytes 与另两者略有差别，也不等于逻辑内容不同；具体 buffer 差异未在用户进程中完成归因。

### C：更小不等于热读更快

ZSTD 减少序列化文件字节，但解码后仍是同样的列数据；本轮热缓存没有证明磁盘带宽是瓶颈。解压增加 CPU 工作是合理机制，然而未做 CPU/IO 分段测量，不能断言观测到的全部 66.3% 增幅都来自解压。NONE 的 44.796 ms 与 A 的 57.2 ms 是不同测量批次，不用它们宣称压缩之外的优化有效。

## 9. 优化与复测

### 已完成：测量方法修正，而非读取算法优化 [S1][S2]

v1 的 `pq.read_table` 路径存在已定位的 dataset 等惰性导入，混入计时；v1 RSS 采样窗口还未严格隔离摘要阶段。v2 将所有 worker 的已知惰性模块前置到 RSS 基线与计时之前，并用 `READY→GO→READ_DONE→SAMPLED→digest→DONE` 把 peak 采样限在读窗口。读取终点和文件不变。这是**测量口径修正**，不是“Parquet 读取优化”。

| 格式 | v1 median ms | v1 max ms | v2 median ms | v2 max ms |
|---|---:|---:|---:|---:|
| CSV | 116.0 | 123.4 | 117.7 | 140.1 |
| Parquet NONE | 54.1 | 1539.3 | 57.2 | 182.8 |
| Arrow IPC | 11.4 | 11.8 | 11.8 | 15.7 |

v2 三种格式的中位数均未变快。Parquet max 从 1539.3 ms 降到 182.8 ms 是该批次观察值，不能证明所有尾部开销都已排除。所有 worker 的 RSS baseline 中位数从约 57.4 MiB 升至约 64.3–64.4 MiB；虽然 ΔRSS 变小，也不能称为读取实现更省内存。v1/v2 的 peak 窗口不同，不直接比较 peak 得出优化收益。

### 已完成：投影与压缩方案评估

B 相对 A 体现了只物化所需列的收益；C 体现了空间节省与热读延迟增加的取舍。两者已有测量与校验记录；本稿未新增实测或修改脚本实现。现有数据无法确认的机制继续保留为局限性。

## 10. 结论及局限性

| 格式 / 方案 | 合理工作负载（选型解释，不全是本轮实测） | 本轮证据与边界 |
|---|---|---|
| CSV | 外部交换、人工查看、兼容性优先的导入导出 | 本轮全列/单列热读均慢于两种二进制格式；不据此否定其互操作价值 |
| Parquet | 数据湖/离线分析、宽表选列、存储成本敏感的批处理 | NONE 已较小，投影收益明显；Row Group 统计可支撑谓词跳过是概念能力，本轮没测过滤/分区裁剪 |
| Arrow 内存格式与 IPC file | Arrow 生态中的中间数据交换、本地高速加载、进程间列式数据传递 | IPC 本轮全列热读最快，但文件最大；单列 RSS 未随 Table 大小同比下降，非通用“零拷贝低内存”保证 |
| Parquet ZSTD level 3 | 更关注持久化空间/传输字节、可接受解压成本的场景 | 本轮省 52.48% 空间，热读中位数慢 66.3%；未测冷读/远端存储，不能据此预测其净收益 |

结论仅适用于这份 100 万行、6 列、无空值、低基数字符串的合成数据及当前 PyArrow 路径。它不是纯布局控制实验，不代表宽表、嵌套类型、高基数、稀疏/空值、多线程、大文件、对象存储、并发或真实训练流水线。没有冷缓存、实际 IO 字节、CPU 分解、写入性能或统计置信区间；5 次结果保留 max，不外推 p99。短读的 2 ms 采样点少，无法保证捕获真正峰值。

### 技术验收记录（2026-11-18）

- **初测 L2**：能解释概念与实验结果，但诊断中的因果链条不足。
- **补强后技术 L3 通过**：2026-11-18 20:10（Asia/Shanghai）。Albert 在补强自测中能提出“IO 节省超过解压成本”的假设；设计对照时保持同一数据及其他条件不变并重复测试；提出采集总耗时、实际磁盘读取量与 IO 耗时、进程 CPU 时间，并以“读盘更少、CPU 时间更多、总耗时更短”的组合支持该假设。
- 上述是**自测中的诊断设计与证据判据**，不是新增实验结果，不表示四份归档 JSON 已包含 IO/CPU 分解，也不改变本报告热读结果。
- **讲解补充（非用户独立作答）**：“仅大小和总耗时不足以拆分因果”。这是验收后的补充讲解，不能计为 Albert 独立说过的内容。
- **仓库交付状态**：已获提交授权；当前材料已整理为待提交包，**PR 待提交，未合并**。技术 L3 通过不等于仓库交付完成。

知识概念与简表见[知识卡片](w07-parquet-arrow-knowledge-card.md)。

---

### 数据来源与 provenance

[S1] v1 热读；[S2] v2 全列（本文 A）；[S3] 单列投影（本文 B）；[S4] 同源 Parquet 压缩（本文 C）。均来自用户提供的原始 JSON；本文只解析现有证据，没有重跑实验。表格取 `files`、`summary`，并用 `trials` 重新计算 median/min/max 交叉校验；正确性取 `validation`，配置取 `environment/limits/writer_config/derived`。

下列为**仓库内只读归档与原附件的映射**。四份 JSON 按原字节复制，不编辑路径、时间、指标或其他字段；内容中的 `/root/data-formats-lab` 仅为历史环境。复制前后 SHA-256 一致。归档位于 `benchmarks/w07-data-formats/`，链接可直接定位：

- **v1**：归档 [`benchmarks/w07-data-formats/results/warm-01/summary.json`](../benchmarks/w07-data-formats/results/warm-01/summary.json)；附件原文件名 `4e33c8241cb71b52e78cafd3fdf8fb3c2b7345d92f76d0fc1e7d7aebffbaced4.json`；JSON 文件 SHA-256：`0f0db7465aba8d36ba66950d38e355b8ab7a067d75a06ac7f1d25e778a763ceb`。
- **v2**：归档 [`benchmarks/w07-data-formats/results/warm-02/summary.json`](../benchmarks/w07-data-formats/results/warm-02/summary.json)；附件原文件名 `b73d4268ec041121bd31e043ab301b6c9cd6f61451a2b6301a6e4458abe2e332.json`；JSON 文件 SHA-256：`678c837559238849691d4161977e54a45d2aa5dcc8f97cf6aca566770f204e17`。
- **projection**：归档 [`benchmarks/w07-data-formats/results/warm-03-col/summary.json`](../benchmarks/w07-data-formats/results/warm-03-col/summary.json)；附件原文件名 `f10cb062d528fefdf4df8806ae8d64a8712ab63f63f60907dde922fb56ad479a.json`；JSON 文件 SHA-256：`c9482f7003f5f9cf022eeedfdf61365d0631a0557d66ebcf6d27b418edf27d8b`。
- **compression**：归档 [`benchmarks/w07-data-formats/results/warm-04-zstd/summary.json`](../benchmarks/w07-data-formats/results/warm-04-zstd/summary.json)；附件原文件名 `71f04d32c59b567edfba2437b50efbd283a809083ebdc4b837d61ceebb57fbab.json`；JSON 文件 SHA-256：`688b40a7c9c2cf2fbaeabf3d45921c7f63d7739197282e4c97ef572e1e52f371`。

复现命令与 schema 已对照所附脚本静态核验；仅完成脚本语法编译检查，未重跑实验。原始数据文件不在材料中，未独立重放 equals、未核验底层实际 IO/CPU；正确性结论依据 JSON 内的校验记录。JSON 内的数据文件 SHA-256 是原实验脚本记录；上述新算的 SHA-256 仅针对四份 JSON 自身。完整文件校验清单见 [FILES-SHA256.txt](../FILES-SHA256.txt)。

[S1]: ../benchmarks/w07-data-formats/results/warm-01/summary.json
[S2]: ../benchmarks/w07-data-formats/results/warm-02/summary.json
[S3]: ../benchmarks/w07-data-formats/results/warm-03-col/summary.json
[S4]: ../benchmarks/w07-data-formats/results/warm-04-zstd/summary.json
