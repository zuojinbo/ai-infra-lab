# W07 数据格式实验

CSV、Parquet、Arrow IPC 的热缓存读取、单列投影和 Parquet 压缩比较。技术 L3 已于 **2026-11-18 20:10（Asia/Shanghai）**补强自测通过；仓库交付为 **PR 待提交，未合并**。验收涉及诊断设计，不新增实验或 IO/CPU 数据。

- [比较报告](../../docs/w07-data-formats-comparison-report.md)：固定十项模板、原始结果、指标口径与技术验收记录。
- [知识卡片](../../docs/w07-parquet-arrow-knowledge-card.md)：概念、真实实验简表与误区。
- [完整文件 SHA-256 清单](../../FILES-SHA256.txt)：覆盖本材料包全部其他文件，不包含清单自身；在仓库根目录执行 `sha256sum -c FILES-SHA256.txt`。

## 目录与依赖

从仓库根目录进入 **`data-formats/w07`** 运行，而不是在 `docs/` 或只读归档目录运行。

```text
docs/
  w07-data-formats-comparison-report.md
  w07-parquet-arrow-knowledge-card.md
data-formats/w07/
  generate_data.py
  benchmark_read_v1.py
  benchmark_read_v2.py
  benchmark_projection.py
  benchmark_compression.py
  README.md
  requirements.txt
  .gitignore
benchmarks/w07-data-formats/results/
  warm-01/summary.json
  warm-02/summary.json
  warm-03-col/summary.json
  warm-04-zstd/summary.json
FILES-SHA256.txt
```

环境要求：**Python >=3.10**，依赖锁定为 **pyarrow==25.0.1、psutil==7.2.2**，见 [requirements.txt](requirements.txt)。原实验为 WSL2 Linux、Python 3.10.12，pandas 未安装。RSS、`/proc` 与 `resource` 测量面向 Linux/WSL2；不保证其他系统同口径。安装与复现依赖所需环境应由执行者准备，材料整理时未运行实验。

```bash
# 从仓库根目录开始
cd data-formats/w07
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

## 数据生成

默认本地数据目录为 **`./data`**。生成器默认即 `data`，读取脚本必须显式指定 `--data-dir ./data`；它们不依赖原实验的绝对路径。

```bash
# 仅在目标数据尚不存在时执行；已有同一份数据则跳过
python generate_data.py --rows 1000000 --seed 42 --output-dir ./data
```

生成 `orders.csv`、`orders.parquet`、`orders.arrow` 和 `manifest.json`。100 万行、seed=42；Parquet NONE，字典编码开启，Row Group=100000。数据文件不入库。生成器使用 `--rows/--output-dir/--seed`，不接受读取脚本的 `--data-dir/--runs`。

## 复现：始终新建 retest 目录

以下命令在 `data-formats/w07` 内运行。新结果写入 **`./results/retest-*`**，不要写到 `../../benchmarks/w07-data-formats/results/`，也不要覆盖旧复测结果；若目录内已有目标文件，另换目录名。

```bash
# A：全列读取，使用 v2 测量口径
python benchmark_read_v2.py --data-dir ./data \
  --output-dir ./results/retest-02 --runs 5 --seed 42 --sample-interval-ms 2

# B：读 API 内只选择金额列，不是全读后 select
python benchmark_projection.py --data-dir ./data \
  --output-dir ./results/retest-03-col --runs 5 --seed 42 \
  --columns amount_cents --sample-interval-ms 2

# C：同源表新写一对 NONE / ZSTD level 3，再进行全列读取
python benchmark_compression.py --data-dir ./data \
  --output-dir ./results/retest-04-zstd --runs 5 --seed 42 --sample-interval-ms 2
```

[benchmark_read_v1.py](benchmark_read_v1.py) 仅作为历史口径保留。v1 的已知惰性模块初始化混入计时、peak 窗口尚未隔离摘要阶段；新增全列结果应使用 [benchmark_read_v2.py](benchmark_read_v2.py)。v2 是测量方法修正，不是读取算法优化，不能把 ΔRSS 下降直接说成代码省内存。

三个读取脚本均产生 `summary.json` 和 `trials.csv`；压缩脚本还生成两份派生 Parquet 与 `derived_manifest.json`。压缩基线必须是本次新写的 NONE，不能拿历史 warm-02 直接与新的 ZSTD 耗时对比。局部 [.gitignore](.gitignore) 忽略虚拟环境、pycache、`data/` 及整个本地 `results/`，防止生成数据或大派生文件被误收录。

## 原始归档与目录映射

报告中缩写的 **`results/warm-*` 统一位于仓库 `benchmarks/w07-data-formats/` 下**。历史实验工作目录 `/root/data-formats-lab` 保留在 JSON 中，仅是原环境记录，不能作为当前可访问文件的保证。

| 原实验输出 | 仓库只读归档（四份用户原始 JSON） | 新复测输出（从本目录运行） |
|---|---|---|
| `/root/data-formats-lab/results/warm-01/summary.json` | [v1](../../benchmarks/w07-data-formats/results/warm-01/summary.json) | 不作为新主基线 |
| `/root/data-formats-lab/results/warm-02/summary.json` | [v2 全列](../../benchmarks/w07-data-formats/results/warm-02/summary.json) | `./results/retest-02/summary.json` |
| `/root/data-formats-lab/results/warm-03-col/summary.json` | [单列投影](../../benchmarks/w07-data-formats/results/warm-03-col/summary.json) | `./results/retest-03-col/summary.json` |
| `/root/data-formats-lab/results/warm-04-zstd/summary.json` | [压缩对照](../../benchmarks/w07-data-formats/results/warm-04-zstd/summary.json) | `./results/retest-04-zstd/summary.json` |

**原始 JSON 不得改值**：四份文件均按原字节复制并设置本地只读权限，Git 不保证保留只读位，因此仍须执行不修改规则及 SHA-256 核对。不要改写 JSON 中的绝对路径、版本、计时或字段，也不要用复测结果覆盖它们。原附件文件名、JSON SHA-256 见[报告 provenance](../../docs/w07-data-formats-comparison-report.md#数据来源与-provenance)。JSON 内 `trials` 已保留各轮；原 `trials.csv` 和 `derived_manifest.json` 未随材料归档，不应声称这些历史文件已入库。

## 证据边界与交付范围

- 四份 JSON 为唯一实测来源，原始指标、慢轮、sum/equals 记录不变；脚本逐字节沿用，不声称新代码优化。
- 同一数据集、单线程、热缓存（每个 worker 前按字节读至 EOF）、非 mmap、`pa.Table` 终点，约 2 ms 采样可能漏峰。不是冷读，也没有实际 IO 或 CPU 分解。
- 补强自测中的“IO 节省超过解压成本”及其测量判据是工程诊断回答，不是新增实测。仅大小和总耗时不足以拆因果是补充讲解，不归为用户独立作答。
- 原始订单数据不在材料内，整理时未独立重放 equals 或读取原数据文件；一致性依据 JSON 内的摘要与分批 equals 记录。复现可从所附生成器重建数据，但新测数字不得冒充原记录。
- 材料自包含于以上文件与清单；不含 `.venv`、pycache、生成数据、派生 Parquet、整理端本地测试结果或凭据。只复制这份清单列出的文件；不改仓库其他内容，不添加运行产物，不修改原始 JSON。
