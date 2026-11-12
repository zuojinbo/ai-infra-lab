# W6 知识卡片：Flink 执行模型、Checkpoint、State、Watermark 与背压

## 这周解决什么问题

看懂 Flink 作业怎么跑、状态怎么存、事件时间怎么推进，以及下游变慢时背压如何向上游传播。用 Kafka → Flink 作业验证：注入慢算子后能定位瓶颈；并测量 Checkpoint 间隔和状态大小对性能的影响。

## 执行模型

- **JobManager** 负责任务调度和 Checkpoint 协调，自己不处理数据。**TaskManager** 是干活的 JVM 进程。
- **Slot** 是 TaskManager 里的资源配额（主要是内存），不是进程，也不是一条完整流水线。一个 Task 要跑起来必须进入某个 Slot。
- **算子** 是逻辑步骤；**Task / subtask** 是该算子的一份执行实例。某个算子的 Task 数等于它的并行度。
- **Slot Sharing**（默认开启）：同一条流水线、并行度一致的多个 Task 可以同时驻留在同一个 Slot 里，它们是并发线程，不是时间片轮转。因此作业通常需要的 Slot 数 ≈ 作业里最大的那个并行度，而不是把各算子并行度相加。
- **算子链（Operator Chaining）**：相邻算子可被捏成同一个 Task、同一条线程，数据用方法调用传递。要在 UI 上分算子看背压，需要关掉算子链，否则 Source、计算和 sink 会收成一个框。
- 读 Kafka 时，Source 并行度一般应对齐分区数：一个分区同时只会被一个子任务读取；并行度小于分区数时，一个子任务会负责多个分区。

## State、Checkpoint、Watermark

- **State** 是作业运行中要跨记录记住的数据，常用的是按 key 分份的 Keyed State。只放内存的话，进程退出状态就丢了。
- **Checkpoint** 是定期把各算子 State 做成一份全局一致快照并持久化。JobManager 发令协调，各 Task 对自己的状态做快照。它解决的是故障恢复，不是时间语义。
- **Watermark** 是随数据向下传的事件时间进度标记，主要作用于窗口和定时器：当 Watermark ≥ 窗口结束时间，窗口才关闭并输出。它不会把线程挂起；普通 map / filter 来一条处理一条，基本不受影响。水位过慢则窗口关得晚、延迟和状态积压增加；过快则窗口关得早，迟到数据可能丢失或不完整。

## 背压

背压是流控，不是报错：下游处理不过来，就一层层让上游少送，避免缓冲撑爆。传播路径是：慢算子 → 其输入缓冲占满 → 上游被拖慢 → 直到 Source 降低从 Kafka 的拉取速度。Kafka 通常仍在正常接收，只是消费变慢、lag 变大。

定位方法：在作业图上从下游往上看，找**最靠下游那个 Busy 高、自身背压为 0** 的算子，它通常就是瓶颈；它上游的背压多半是被它堵住的。除了算子本身慢，还要排除并行度过小、Checkpoint 过频或状态过大、sink 写不出去、机器资源不够。

## 实验

环境：VMware 实验机，Docker 部署 Flink 1.20.1（1 个 JobManager + 1 个 TaskManager、2 个 Slot）与单节点 Kafka 3.8.1。作业并行度均为 1。Flink 通过共享网络以 `kafka:19092` 连接 Kafka。按实验模板只改一个变量。

### 1. 基线：Kafka → Flink 原样转发

链路打通：`flink-in` 的消息可原样出现在 `flink-out`。算子默认链在一起。Kafka producer 压测写入 20 万条（每条 200 字节）约 13.0 万条/秒；作业图背压 0%、Busy 约 1%。说明无慢算子时链路很轻松。注意：producer 统计的是写入 Kafka 的速度，不是 Flink 处理速度。

### 2. 注入慢算子并定位背压

唯一改动：增加每条 `sleep(5ms)` 的慢算子，并关闭算子链。作业图拆成 Source → Calc / grow-state → sink Writer → sink Committer。持续限速写入期间，Calc（或 grow-state）Busy 升至 100%、自身背压 0%；Source 背压升至 100%、自身 Busy 0%；sink 两侧均为 0%。结论：瓶颈就是注入的慢算子，背压从它传到 Source，不是 sink 写不出去。

### 3. Checkpoint 与状态大小

同一类慢作业打开 Checkpoint（间隔 10 秒）：

| 条件 | 快照大小 | 端到端耗时 | 说明 |
| --- | --- | --- | --- |
| 慢算子无累积状态 | 约 1.3 KB | 7 ms | 多次全部成功；开销可忽略 |
| 按 key 堆积状态（上限约 32×2MB） | 64.0 MB | 6 s 990 ms | 耗时几乎都在 `grow-state` |

状态很小、对齐又快时，打开 Checkpoint 不一定拖慢作业。状态变大后，每次快照要写出几十 MB，耗时从毫秒级升到数秒。

实验中途出现过 Kafka 询问分区位移超时（60s），作业失败后从最近一次成功 Checkpoint 恢复（Restored = 1）。这与快照大小无关，说明 Checkpoint 的恢复路径是有效的。

### 4. Checkpoint 间隔

在大状态前提下，把间隔从 10 秒改为 2 秒：最近一次完成的快照约 41.5 MB，端到端耗时 53.6 秒，仍几乎都花在 `grow-state`。一次快照尚未写完，下一次已该触发，快照排队，有效处理时间被切碎。作业图仍是 `grow-state` Busy 100%、Source 背压 100%。

## 结论与局限

- 看背压必须先拆开算子链；链在一起时只能看到整框，不能定位到具体算子。
- 定位规则：最下游 Busy 高且自身无背压的算子是瓶颈。
- Checkpoint 开销主要由状态大小和间隔共同决定：小状态时 10 秒间隔开销可忽略；大状态 + 过短间隔会让快照时间超过触发周期，明显伤害吞吐。
- 局限：并行度固定为 1；慢算子用 `sleep` 模拟，不等于真实 CPU / IO 瓶颈；大状态用追加字节制造体积，不是业务状态形态；未系统对比不同状态后端（如 HashMap 与 RocksDB）；部分吞吐数字来自 Kafka producer，解释 Flink 性能时应看作业图与 Checkpoint 指标。

## 关键命令与入口

- Flink Web UI：`http://localhost:8081`
- 作业目录：`~/flink-lab`；Kafka 目录：`~/kafka-lab`
- 取消作业：`docker exec flink-lab-jobmanager-1 /opt/flink/bin/flink cancel <JobID>`
