# W2 知识卡片：Linux 内存管理、Page Cache、Swap 与磁盘 I/O

## 1. 虚拟内存与缺页

- 每个进程有独立的虚拟地址空间，内核通过页表把虚拟页（通常 4KB）映射到物理页。
- `malloc` 只分配虚拟地址，**第一次写入时**才触发缺页、分配物理页（按需分配）。所以 VIRT 可以远大于物理内存。
- 缺页（page fault）是 CPU 触发的**异常**，不是中断：
  - **minor fault**：物理页已在内存中（例如在 Page Cache 里，或刚分配的新页），只需建立映射，开销为微秒级。数量多通常是正常现象（新分配内存、fork、读已缓存的文件）。
  - **major fault**：需要从磁盘（文件或 Swap）读取数据，开销大，持续偏高说明内存不足。
- 查看：`pidstat -r 1` 的 `minflt/s`、`majflt/s`。

## 2. 进程内存的几种口径

| 指标 | 含义 | 用途 |
|---|---|---|
| VSZ / VIRT | 进程申请的全部虚拟地址 | 参考意义有限 |
| RSS / RES | 驻留在物理内存的页，**共享页每个进程都算一整份** | 多个进程相加会偏大 |
| PSS | 共享页按共享进程数平摊 | 评估服务真实内存占用，所有进程相加约等于真实用量 |
| USS | 进程独占的页 | 评估杀掉进程能释放多少 |

查看：`cat /proc/<PID>/smaps_rollup`，或 `smem -k`。容器内存限制按 cgroup 统计，口径又不同（W12）。

## 3. Page Cache

- 读文件后数据页留在内存，再次读取直接命中，不访问磁盘。
- 写文件默认先写进 Page Cache 并标记为 dirty page，由内核线程异步回写（受 `vm.dirty_ratio`、`vm.dirty_background_ratio` 控制）。查看：`grep -E 'Dirty|Writeback' /proc/meminfo`。
- `write()` 返回成功**不代表已落盘**。要保证持久化需调用 `fsync()` / `fdatasync()`，或用 `O_SYNC` 打开文件；代价是单次延迟变长、吞吐大幅下降，数据库常用组提交（group commit）摊薄成本。
- `free -h`：**free** 小是正常的，空闲内存被拿去做缓存了；真正要看 **available**（不触发 Swap 就能给新程序用的内存，含可回收缓存）。
- 扩容信号：available 持续走低、出现 Swap 换入换出、major fault 增多、发生 OOM。

## 4. 内存回收、Swap 与 OOM

- 内存不足时内核回收：丢弃干净的 Page Cache、回写 dirty page、把匿名内存（堆、栈）换到 Swap。
- **`so`（swap out）**：内存吃紧，把冷的匿名页从内存写到 Swap，属于**预警**。
- **`si`（swap in）**：进程访问之前被换出的页，触发 major fault 从 Swap 读回，进程阻塞等待磁盘，**已经在影响性能**。
- **`si`、`so` 同时持续很高 = 颠簸（thrashing）**：进程都卡在换页上，`us` 很低、`wa` 很高，服务基本无响应。
- `vm.swappiness`（0–200，默认 60）不是"剩多少内存开始 Swap"的阈值，而是回收匿名内存和回收 Page Cache 之间的**相对倾向**。默认 60 时，即使还有大量缓存，内核也会同时换出匿名页；另外 dirty page 和热页不能直接回收。延迟敏感的服务（数据库、Kafka、Flink）常调到 1–10 或关闭 Swap，K8s 节点默认要求关闭 Swap。
- 回收不出内存时 OOM Killer 按 `oom_score` 杀进程，查看：`dmesg | grep -i oom`。K8s 的 OOMKilled 本质是 cgroup 内存超限。

### 实验记录：stress-ng 内存压力（VMware 虚拟机，swappiness=60）

`stress-ng --vm 2 --vm-bytes 90% --timeout 60s` 时 `vmstat 1` 的一行：

```
r b   swpd   free   buff    cache  si     so  bi     bo   in   cs us sy id wa st
2 0 357492 423956 561420 18752676  0 357380   0 357380 5745 3432  2  9 88  1  0
```

- `so` 与 `bo` 相等（约 350MB/s）：写盘全部来自 Swap 换出。
- `si` 为 0：stress-ng 不断申请新内存，被换出的页尚未被再次访问。
- `wa` 仅 1、`b` 为 0：盘很快，CPU 几乎不用等 IO；**`wa` 低不代表没问题，瓶颈在内存**。
- cache 仍有约 18GB 却已经 Swap：swappiness=60 两头一起回收，部分缓存是 dirty page 或热页；虚拟机宿主机的内存气球也可能放大现象。

## 5. 磁盘 I/O

- 三个不同的指标：**吞吐**（MB/s，顺序读写关注）、**IOPS**（次/秒，随机小块读写关注）、**延迟**（`iostat -x` 的 `r_await`/`w_await`，毫秒）。
- `aqu-sz`：平均队列长度，越大排队越严重。
- `%util`：设备忙碌时间占比。机械盘接近 100% 基本等于饱和；**SSD/NVMe 能并发处理很多请求，100% 不代表饱和**。
- **判断饱和**：继续加压时，吞吐不再增长、延迟明显上升、队列变长，才是真正的饱和点（可用 `fio` 逐步增加 `iodepth` 验证）。
- 吞吐不变但 `r_await`、`aqu-sz` 大幅上升：请求在排队，磁盘饱和或被其他负载抢占（如虚拟机邻居）。用 `pidstat -d 1` 或 `iotop` 定位进程（`pidstat` 默认只看 CPU）。
- 顺序读远快于随机读，是 W7 Parquet 列式存储、Row Group 设计的源头。

## 6. 快速判断瓶颈

| 信号 | 先怀疑 |
|---|---|
| `wa` 高，`b` 大于 0 | I/O |
| `si/so` 不为 0、`majflt` 偏高、available 很低 | 内存 |
| `us+sy` 接近满、`r` 远大于核数 | CPU（W1） |

注意：Swap 导致的 `wa` 高是结果，不是原因，根源在内存，换更快的盘只能缓解症状。

## 7. 关联知识：Kafka 为什么不逐条 fsync 也可靠

- Kafka 的日志本身就是数据，没有额外的预写日志，可靠性来自**多副本**。
- 流程：Leader 先写进自己的 Page Cache → Follower **主动 Fetch 拉取**并写进自己的 Page Cache → Leader 根据 Follower 上报的 offset 推进高水位（HW）→ 回复生产者成功。消费者只能读到 HW 以内的消息。
- `acks=all`：等 **ISR 里的同步副本**都拿到才确认，需配合 `min.insync.replicas=2`。**生产者收到成功确认的消息不会丢**；确认前 Leader 就宕机的消息由生产者重试，可能重复，开启 `enable.idempotence=true` 可去重。
- `acks=1`：Leader 写进 Page Cache 就确认，不等 Follower；Leader 宕机时，**已确认的消息也可能丢**，生产者无从得知。
- 判断会不会丢的关键：**生产者收到确认的那一刻，有几份副本已经拿到了消息**。

## 8. Benchmark 注意事项（本周所学）

- 标明冷缓存还是热缓存。测磁盘前执行 `sync; echo 3 > /proc/sys/vm/drop_caches`，或使用 `O_DIRECT`（`dd oflag=direct`、`fio --direct=1`）。
- 测试数据量要明显大于可用内存，或用 cgroup 固定可用内存，否则哪种格式更容易被完整缓存会决定结果。
- 每组至少重复 3 次，记录内存峰值。
- `/tmp` 可能是 tmpfs（`df -h /tmp` 确认），写在那里不会落盘。

## 9. 常见的坑

- 把 free 小当成内存不足，忽略 available。
- 把 minor fault 多当成内存问题。
- 只看 `%util` 判断 NVMe 是否饱和。
- 看到 `wa` 高就认为盘慢，忽略了 Swap 这个根源。
- 把多个进程的 RSS 直接相加估算内存。
