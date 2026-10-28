# W3 知识卡片：Linux perf 与火焰图

> 周次：W3（2026-10-27 ~ 11-02）｜环境：VMware Ubuntu 24.04，内核 7.0.0-38-generic，perf 7.0.14，shell 为 zsh
> 目标：用 `perf` 做 CPU 采样，读懂火焰图，把瓶颈从「哪个进程」定位到「哪个函数」。

---

## 1. 采样 vs 追踪

| | 采样（sampling） | 追踪（tracing） |
|---|---|---|
| 做法 | 按固定频率打断 CPU，记录「此刻在跑什么 + 调用栈」 | 每一次事件（函数进入/退出、系统调用等）都记录 |
| 类比 | 每秒扫一眼教室，看谁在说话 | 给每个人装录音笔 |
| 开销 | 小，可在线上短时使用 | 大，容易拖慢程序 |
| 结果 | 统计意义上的占比 | 完整、精确的事件序列 |

`perf record` 默认就是**采样**（VM 里事件为 `task-clock`，物理机上一般是 `cycles`）。

采样频率的取舍：
- **太低**：短热点采不到，结果有偏差。
- **太高**：采样本身抢 CPU，测到的是「被监控拖慢的程序」。
- 可用 `-F 99` 这类频率；用 99 而不是 100，可以避开和定时任务同频造成的偏差。

## 2. 调用栈（call stack）

采样记录的是**整条调用链**，不是单个函数名，例如：

```
_start → __libc_start_main → main → mid → burn
```

`perf record -g` 才会记录调用栈。栈要完整，程序最好带 `-g`（调试符号）和 `-fno-omit-frame-pointer`（保留帧指针）编译。

## 3. 怎么读 on-CPU 火焰图

- **纵轴**：调用关系，下面是调用者，上面是被调用者。
- **横轴**：**不是时间顺序**，而是采样次数的占比。越宽，说明占 CPU 时间越多。
- 同一层的方块按字母排序，左右位置没有意义。
- 颜色默认是随机暖色，没有含义。

一句话：**宽 = 热，高 = 调用链深。**

读图步骤：
1. 先找最宽的几块，它们是热点候选。
2. 看最宽的是不是**顶层的叶子**（函数自己在算），还是只是下面某个父函数很宽（它在调用别人）。
3. 区分两类「宽」：
   - **自身贵**：函数本身指令多、计算重，应优化算法或实现。
   - **被叫得多但自身便宜**：小函数被调用的次数爆炸，应往下看是谁在频繁调用它。

### Children vs Self（`perf report`）
- **Self**：函数**自己**执行的时间占比，是真正烧 CPU 的部分。
- **Children**：自己加上它调用的所有子函数的时间占比。
- 实测：`burn` 的 Self 为 99.95%；`main`/`mid` 的 Children 约 100%、Self 约 0%，说明它们只是在调用路径上。

### on-CPU 图的局限
on-CPU 图只回答「CPU 在忙什么」。**CPU 不高、但延迟很高**时（等锁、等磁盘、等网络、被调度出去），这些「睡着」的时间在图上看不见，需要 off-CPU 分析。

## 4. 环境检查清单

```bash
# 安装（Ubuntu）
sudo apt install -y linux-tools-common linux-tools-generic linux-tools-$(uname -r)
perf --version                       # 实测 perf version 7.0.14

# 权限
cat /proc/sys/kernel/perf_event_paranoid
sudo sysctl -w kernel.perf_event_paranoid=1   # 临时放开，重启失效
```

`perf_event_paranoid` 的含义：值越大越严格。为 2 及以上时，普通用户基本无法采样；1 允许普通用户采样自己的进程；0 或 -1 可以看内核和全系统（不建议在生产长期开启）。

常见坑：
| 现象 | 原因 | 处理 |
|---|---|---|
| `Permission denied` | `perf_event_paranoid` 太高 | 调低该值，或用 `sudo` |
| 栈里大量 `[unknown]`、没有用户态符号 | 二进制被 strip 了、没有调试符号、省略了帧指针 | 编译加 `-g -fno-omit-frame-pointer`；或用 `--call-graph dwarf` |
| `cycles`/`instructions` 显示 `<not supported>` | 虚拟机没有开放硬件性能计数器（PMU） | 改用 `task-clock`；或在 VMware 开启「虚拟化 CPU 性能计数器」 |
| `-p` 报 `requires a value` | `pgrep` 没找到进程，结果为空 | 先确认进程在运行 |
| zsh 下 `(pgrep …)` 报 glob 错误 | `(…)` 是 fish 语法 | zsh/bash 用 `$(pgrep -n hot)` |

## 5. 工具链

```bash
perf top -p <pid>                         # 实时查看热点函数
perf record -g -p <pid> -- sleep 10       # 对运行中的进程采样 10 秒
perf record -g -o out.data ./prog         # 对一次运行全程采样
perf report --stdio                       # 文字报告（Children/Self）
perf stat -e task-clock ./prog            # 统计计数，用来比较「花了多少 CPU」

# 火焰图（Brendan Gregg FlameGraph）
git clone --depth=1 https://github.com/brendangregg/FlameGraph.git ~/FlameGraph
perf script | ~/FlameGraph/stackcollapse-perf.pl | ~/FlameGraph/flamegraph.pl --title "xxx" > out.svg
```

| 工具 | 回答的问题 |
|---|---|
| `perf stat` | 一共花了多少（CPU 时间、指令数等） |
| `perf record/report` | 时间花在哪些函数、哪些调用栈上 |
| 火焰图 | 把 `record` 的结果画成一张图，方便一眼看全 |

其他好用的查看工具：Hotspot（图形界面，直接打开 `perf.data`）、speedscope、Firefox Profiler；按语言有 py-spy（Python）、async-profiler（Java）；生产环境的持续采样有 Pyroscope、Parca。

## 6. 实验：只改编译优化级别（-O0 → -O2）

按 ai-infra-lab 实验模板记录。

- **研究问题**：`-O2` 能否明显减少完成同样计算量所需的 CPU 时间？
- **假设**：能，`burn` 的循环会被编译器优化。
- **环境**：VMware Ubuntu 24.04，内核 7.0.0-38，perf 7.0.14，gcc，单线程。
- **负载**（有限工作量，结果 `sink` 两边一致，可用来校验）：

```c
#include <stdio.h>
volatile long sink;
void burn(long n){ for(long i=0;i<n;i++) sink += i*i; }
void mid(long n){ for(int k=0;k<50;k++) burn(n); }
int main(void){
  for(int r=0;r<200;r++) mid(200000);
  printf("done sink=%ld\n", sink);
  return 0;
}
```

- **Baseline**：`gcc -g -fno-omit-frame-pointer -O0`
- **唯一变量**：`-O0` 改为 `-O2`，其余条件不变。
- **指标**：`perf stat` 的 `task-clock` 和 `seconds time elapsed`；火焰图热点栈。

### 结果

| 指标 | -O0 | -O2 | 变化 |
|---|---|---|---|
| task-clock | 1947.65 ms | 1277.42 ms | 下降约 34% |
| time elapsed | 1.949 s | 1.279 s | 下降约 34% |
| user / sys | 1.948 s / 0.001 s | 1.277 s / 0.002 s | 几乎全在用户态 |
| perf record 样本数 | 15938 | 10639 | 运行变短，样本减少 |
| 输出 sink | 8219722593290448384 | 8219722593290448384 | 一致，工作量相同 |

`cycles`/`instructions` 在 VMware 中为 `<not supported>`，本次没有使用。

### 分析
- 同样的计算量下 CPU 时间下降约三分之一，假设成立。
- `task-clock` ≈ `elapsed`，且 sys 接近 0：单线程纯计算，没有 I/O 等待，也没有被抢占。
- baseline 火焰图（无限循环版本，采样 10 秒）几乎是一根竖条：`hot → … → main → mid → burn`，每层都接近 100% 宽。这是因为程序只做一件事，并不是异常。
- 判断优化是否成功，要看**同样工作量花的时间变少**，而不是「图必须变窄」。只要 CPU 一直在忙，热点在图上的占比就可能仍接近 100%。
- `-O2` 可能把 `burn` 内联进 `mid`，此时图上顶层的函数名会变化，属于正常现象。

### 结论与局限
- 结论：仅改 `-O2`，CPU 时间下降约 34%。
- 局限：每种配置只跑了 1 次，没有多次取平均和方差；VM 中无法使用硬件计数器（cycles/instructions/IPC）；负载是合成的微基准，不代表真实业务。

## 7. 本周要点速记
1. `perf record` 默认是采样；频率太低会漏掉热点，太高会干扰被测程序。
2. 火焰图横轴是占比，不是时间；**宽 = 热**。
3. 优先看 **Self** 高的叶子函数；Children 高、Self 低的函数只是在调用路径上。
4. 出现 `[unknown]` 时，先检查调试符号和帧指针。
5. on-CPU 图看不到等待时间；CPU 低但延迟高时，要用 off-CPU 分析。
6. 优化前后的对比要用**固定工作量**和 `perf stat` 的时间指标，并且一次只改一个变量。
