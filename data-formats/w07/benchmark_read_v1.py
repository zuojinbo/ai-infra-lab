#!/usr/bin/env python3
"""热缓存下，把 CSV / Parquet / Arrow IPC 全量读成内存中的 pyarrow.Table。

依赖：Python 标准库 + pyarrow + psutil（不要额外安装 numpy）。
需与 generate_data.py 使用同一套 schema 和文件名。

设计要点：
- 每个测量在独立子进程中进行；父进程先把该文件顺序读到 EOF，再启动 worker。
- 计时只用 time.perf_counter，覆盖“读取 + 物化成 Table”，不含 import、预热、校验。
- 单线程：pyarrow.set_cpu_count(1)、set_io_thread_count(1)，并显式 use_threads=False。
- CSV 使用与生成时相同的显式 schema；Parquet memory_map=False；
  Arrow IPC 用 pa.OSFile（不用 mmap）再 read_all，避免惰性映射被当成已读完。
- 内存主指标：仍持有 Table 时的 RSS − import 后基线。
  sampled_peak 是父进程轮询到的峰值，可能漏尖峰；
  ru_maxrss 是进程生命周期高水位，不是从基线重新起算的读取峰值。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

MIB = 1024 * 1024
DEFAULT_RUNS = 5
MIN_RUNS = 3
DEFAULT_SEED = 42
DEFAULT_SAMPLE_INTERVAL_MS = 2
VALIDATE_BATCH_SIZE = 100_000
WORKER_TIMEOUT_S = 600

# 与 generate_data.py 保持一致。
SCHEMA_FIELDS = [
    ("order_id", "int64"),
    ("user_id", "int64"),
    ("amount_cents", "int64"),
    ("city", "string"),
    ("event_time", "int64"),
    ("category", "string"),
]
FILES = {
    "csv": "orders.csv",
    "parquet": "orders.parquet",
    "arrow_ipc": "orders.arrow",
}
FORMATS = ("csv", "parquet", "arrow_ipc")
MANIFEST_NAME = "manifest.json"
RESULT_JSON = "summary.json"
RESULT_CSV = "trials.csv"


def schema():
    import pyarrow as pa

    return pa.schema([pa.field(name, getattr(pa, typ)()) for name, typ in SCHEMA_FIELDS])


def csv_convert_options():
    import pyarrow as pa
    import pyarrow.csv as pacsv

    sch = schema()
    return pacsv.ConvertOptions(column_types={field.name: field.type for field in sch})


def bytes_mib(n: int) -> float:
    return n / MIB


def median_min_max(values: list[float]) -> dict:
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def configure_single_thread() -> dict:
    import pyarrow as pa

    pa.set_cpu_count(1)
    if hasattr(pa, "set_io_thread_count"):
        pa.set_io_thread_count(1)
    return {
        "pyarrow_cpu_count": int(pa.cpu_count()),
        "pyarrow_io_thread_count": int(pa.io_thread_count())
        if hasattr(pa, "io_thread_count")
        else None,
        "use_threads": False,
    }


def as_py(value):
    if value is None:
        return None
    if hasattr(value, "as_py"):
        return value.as_py()
    return value


def logical_digest(table) -> dict:
    """对 Table 做确定性列聚合，作为逻辑内容摘要。不比较物理 buffer。"""
    import pyarrow as pa
    import pyarrow.compute as pc

    columns = []
    for field in table.schema:
        col = table.column(field.name)
        item = {
            "name": field.name,
            "type": str(field.type),
            "null_count": int(col.null_count),
        }
        if pa.types.is_integer(field.type):
            item["sum"] = as_py(pc.sum(col))
            item["min"] = as_py(pc.min(col))
            item["max"] = as_py(pc.max(col))
        elif (
            pa.types.is_string(field.type)
            or pa.types.is_large_string(field.type)
            or pa.types.is_binary(field.type)
        ):
            item["min"] = as_py(pc.min(col))
            item["max"] = as_py(pc.max(col))
            item["utf8_length_sum"] = as_py(pc.sum(pc.utf8_length(col)))
        else:
            raise RuntimeError("暂不支持的列类型：{} {}".format(field.name, field.type))
        columns.append(item)
    return {"num_rows": int(table.num_rows), "columns": columns}


def read_table(fmt: str, path: Path):
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.ipc as paipc
    import pyarrow.parquet as pq

    if fmt == "csv":
        return pacsv.read_csv(
            path,
            read_options=pacsv.ReadOptions(use_threads=False, encoding="utf8"),
            convert_options=csv_convert_options(),
        )
    if fmt == "parquet":
        return pq.read_table(
            path,
            memory_map=False,
            use_threads=False,
            pre_buffer=False,
        )
    if fmt == "arrow_ipc":
        options = paipc.IpcReadOptions(use_threads=False)
        with pa.OSFile(str(path), "rb") as source:
            reader = paipc.open_file(source, options=options)
            return reader.read_all()
    raise ValueError("未知格式：{}".format(fmt))


def iter_source_batches(fmt: str, path: Path):
    """流式取出 RecordBatch；CSV 的块大小可能与其他格式不同，调用方必须再切齐。"""
    import pyarrow as pa
    import pyarrow.csv as pacsv
    import pyarrow.ipc as paipc
    import pyarrow.parquet as pq

    if fmt == "csv":
        reader = pacsv.open_csv(
            path,
            read_options=pacsv.ReadOptions(use_threads=False, encoding="utf8"),
            convert_options=csv_convert_options(),
        )
        for batch in reader:
            yield batch
        return
    if fmt == "parquet":
        parquet_file = pq.ParquetFile(path, memory_map=False)
        for batch in parquet_file.iter_batches(batch_size=VALIDATE_BATCH_SIZE, use_threads=False):
            yield batch
        return
    if fmt == "arrow_ipc":
        options = paipc.IpcReadOptions(use_threads=False)
        with pa.OSFile(str(path), "rb") as source:
            reader = paipc.open_file(source, options=options)
            for index in range(reader.num_record_batches):
                yield reader.get_batch(index)
        return
    raise ValueError("未知格式：{}".format(fmt))


def iter_fixed_tables(fmt: str, path: Path, batch_size: int):
    """把任意来源切成固定行数的 Table，避免 CSV chunk 大小影响逐批 equals。"""
    import pyarrow as pa

    pending = []
    pending_rows = 0
    for batch in iter_source_batches(fmt, path):
        if batch.num_rows == 0:
            continue
        pending.append(batch)
        pending_rows += batch.num_rows
        while pending_rows >= batch_size:
            table = pa.Table.from_batches(pending, schema=schema())
            yield table.slice(0, batch_size)
            rest = table.slice(batch_size)
            pending = rest.to_batches() if rest.num_rows else []
            pending_rows = rest.num_rows
    if pending_rows:
        yield pa.Table.from_batches(pending, schema=schema())


def linux_ru_maxrss_bytes() -> dict:
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF)
    raw = int(usage.ru_maxrss)
    # Linux 上 ru_maxrss 的单位是 KB；不能把它说成“从基线重置后的读取峰值”。
    return {
        "ru_maxrss_raw": raw,
        "ru_maxrss_unit": "kilobytes_on_linux",
        "ru_maxrss_bytes": raw * 1024,
    }


def worker_main(args: argparse.Namespace) -> int:
    import psutil
    import pyarrow as pa
    import pyarrow.compute as pc  # noqa: F401  与其它读库一并导入，保证基线可比
    import pyarrow.csv as pacsv  # noqa: F401
    import pyarrow.ipc as paipc  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401

    thread_info = configure_single_thread()
    process = psutil.Process()
    rss_baseline = int(process.memory_info().rss)

    sys.stdout.write("READY\n")
    sys.stdout.flush()
    go = sys.stdin.readline()
    if go.strip() != "GO":
        raise SystemExit("worker 未收到 GO，实际收到：{!r}".format(go))

    started = time.perf_counter()
    table = read_table(args.format, Path(args.input))
    num_rows = int(table.num_rows)
    table_nbytes = int(table.nbytes)
    buffer_size = int(table.get_total_buffer_size()) if hasattr(table, "get_total_buffer_size") else None
    elapsed_s = time.perf_counter() - started

    # 计时结束后、仍持有 table 时记录 RSS，再做摘要；摘要不计入 elapsed_s。
    rss_end = int(process.memory_info().rss)
    ru = linux_ru_maxrss_bytes()
    digest_started = time.perf_counter()
    digest = logical_digest(table)
    digest_s = time.perf_counter() - digest_started

    expected = schema()
    if table.schema != expected:
        # 允许字段相等但 metadata 细微差异时用 equals 再确认。
        if not table.schema.equals(expected, check_metadata=False):
            raise RuntimeError("读回 schema 与约定不一致：{}".format(table.schema))

    result = {
        "format": args.format,
        "input": str(Path(args.input)),
        "elapsed_s": elapsed_s,
        "digest_s": digest_s,
        "num_rows": num_rows,
        "table_nbytes": table_nbytes,
        "table_get_total_buffer_size": buffer_size,
        "rss_baseline_bytes": rss_baseline,
        "rss_end_bytes": rss_end,
        "rss_end_delta_bytes": rss_end - rss_baseline,
        "thread_info": thread_info,
        "digest": digest,
        "pyarrow_version": pa.__version__,
        "psutil_version": __import__("psutil").__version__,
        **ru,
    }
    Path(args.result_file).write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    # 保持对 table 的引用直到进程即将退出，避免过早释放影响 ru_maxrss 解读。
    if table.num_rows != num_rows:
        raise RuntimeError("内部错误：table 在写结果前被改动")
    sys.stdout.write("DONE\n")
    sys.stdout.flush()
    return 0


def warm_cache(path: Path, block_size: int = 8 * 1024 * 1024) -> int:
    """顺序把文件读到 EOF，只为加热页缓存；不做哈希，以免干扰缓存位置。"""
    total = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(block_size)
            if not chunk:
                break
            total += len(chunk)
    return total


def target_result_paths(output_dir: Path) -> list[Path]:
    return [output_dir / RESULT_JSON, output_dir / RESULT_CSV]


def refuse_if_exists(output_dir: Path) -> None:
    existing = [path for path in target_result_paths(output_dir) if path.exists()]
    if not existing:
        return
    lines = [
        "结果文件已存在，拒绝覆盖，以免毁掉上次实验记录：",
        *["  - {}".format(path) for path in existing],
        "请更换 --output-dir，例如 ./results/warm-02。",
    ]
    raise SystemExit("\n".join(lines))


def default_output_dir() -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return Path("results") / "warm-{}".format(stamp)


def load_data_files(data_dir: Path) -> dict:
    missing = [data_dir / name for name in FILES.values() if not (data_dir / name).exists()]
    if missing:
        raise SystemExit(
            "在 {} 中找不到数据文件：\n{}".format(
                data_dir,
                "\n".join("  - {}".format(path) for path in missing),
            )
        )
    files = {}
    for fmt, name in FILES.items():
        path = data_dir / name
        files[fmt] = {
            "format": fmt,
            "name": name,
            "path": path,
            "bytes": path.stat().st_size,
        }
    manifest_path = data_dir / MANIFEST_NAME
    manifest = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for fmt, info in files.items():
            recorded = manifest.get("files", {}).get(info["name"], {}).get("bytes")
            info["manifest_bytes"] = recorded
            if recorded is not None and recorded != info["bytes"]:
                raise SystemExit(
                    "文件大小与 manifest 不一致：{} 实际 {} 字节，manifest 记录 {} 字节。".format(
                        info["path"], info["bytes"], recorded
                    )
                )
    return {"files": files, "manifest": manifest, "manifest_path": manifest_path if manifest else None}


def build_schedule(n_runs: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    schedule = []
    trial = 0
    for rep in range(1, n_runs + 1):
        order = list(FORMATS)
        rng.shuffle(order)
        for fmt in order:
            trial += 1
            schedule.append({"trial": trial, "rep": rep, "format": fmt})
    return schedule


def run_worker(
    *,
    fmt: str,
    path: Path,
    result_file: Path,
    sample_interval_s: float,
) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--format",
        fmt,
        "--input",
        str(path),
        "--result-file",
        str(result_file),
    ]
    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = "1"
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"

    import psutil

    started = time.perf_counter()
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        bufsize=1,
    )
    child = psutil.Process(proc.pid)
    samples = []
    sample_error = None
    try:
        ready = proc.stdout.readline()
        if ready.strip() != "READY":
            stdout, stderr = proc.communicate(timeout=30)
            raise RuntimeError(
                "worker 未发出 READY。\nstdout={!r}\nstderr={}".format(
                    ready + (stdout or ""),
                    stderr,
                )
            )
        # 先开始采样再发出 GO，尽量覆盖整个读取过程。
        try:
            samples.append(int(child.memory_info().rss))
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            sample_error = str(exc)
        proc.stdin.write("GO\n")
        proc.stdin.flush()
        deadline = time.time() + WORKER_TIMEOUT_S
        while True:
            try:
                samples.append(int(child.memory_info().rss))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                break
            if proc.poll() is not None:
                break
            if time.time() > deadline:
                proc.kill()
                raise RuntimeError("worker 超时（{} 秒）：{}".format(WORKER_TIMEOUT_S, fmt))
            time.sleep(sample_interval_s)
        stdout, stderr = proc.communicate(timeout=30)
    except Exception:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        raise
    wall_s = time.perf_counter() - started
    if proc.returncode != 0:
        raise RuntimeError(
            "worker 退出码 {}。\nstdout={}\nstderr={}".format(
                proc.returncode, stdout, stderr
            )
        )
    if not result_file.exists():
        raise RuntimeError("worker 未写出结果文件。stderr={}".format(stderr))
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    payload["sampled_peak_rss_bytes"] = max(samples) if samples else None
    payload["sampled_rss_sample_count"] = len(samples)
    payload["sample_interval_s"] = sample_interval_s
    payload["sample_error"] = sample_error
    payload["parent_wall_s"] = wall_s
    payload["worker_stdout"] = stdout.strip()
    payload["worker_stderr"] = stderr.strip()
    return payload


def run_strong_validation(data_dir: Path, result_file: Path) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--validate",
        "--data-dir",
        str(data_dir),
        "--result-file",
        str(result_file),
    ]
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=WORKER_TIMEOUT_S,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            "独立校验进程失败，退出码 {}。\nstdout={}\nstderr={}".format(
                proc.returncode, proc.stdout, proc.stderr
            )
        )
    return json.loads(result_file.read_text(encoding="utf-8"))


def validate_main(args: argparse.Namespace) -> int:
    configure_single_thread()
    data = load_data_files(Path(args.data_dir).expanduser().resolve())
    files = data["files"]
    batch_size = VALIDATE_BATCH_SIZE
    iterators = {
        fmt: iter_fixed_tables(fmt, info["path"], batch_size) for fmt, info in files.items()
    }
    compared = 0
    total_rows = 0
    while True:
        batches = {}
        ended = []
        for fmt, iterator in iterators.items():
            try:
                batches[fmt] = next(iterator)
            except StopIteration:
                ended.append(fmt)
        if ended:
            if len(ended) != len(iterators):
                raise RuntimeError(
                    "三种格式的行数不一致：先结束的是 {}，本批已读到的格式是 {}。".format(
                        ended, list(batches)
                    )
                )
            break
        rows = {fmt: table.num_rows for fmt, table in batches.items()}
        if len(set(rows.values())) != 1:
            raise RuntimeError("切齐后仍出现不同行数：{}".format(rows))
        # 以 Arrow IPC 为对照，避免在校验进程里再常驻三份完整表。
        reference = batches["arrow_ipc"]
        for fmt, table in batches.items():
            if fmt == "arrow_ipc":
                continue
            if not table.equals(reference):
                raise RuntimeError(
                    "第 {} 批（累计已比较 {} 行之后）{} 与 arrow_ipc 的逻辑内容不一致。".format(
                        compared + 1, total_rows, fmt
                    )
                )
        compared += 1
        total_rows += reference.num_rows

    if total_rows == 0:
        raise RuntimeError("校验读到 0 行，数据文件可能为空。")
    report = {
        "passed": True,
        "batch_size": batch_size,
        "batches_compared": compared,
        "total_rows": total_rows,
        "pairs": ["csv==arrow_ipc", "parquet==arrow_ipc"],
        "note": (
            "在独立进程中按固定行数切批后做 table.equals；"
            "先把 CSV / Parquet / IPC 的原始 chunk 切齐，再比较逻辑内容。"
        ),
    }
    Path(args.result_file).write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


def summarize(trials: list[dict]) -> dict:
    out = {}
    for fmt in FORMATS:
        rows = [item for item in trials if item["format"] == fmt]
        out[fmt] = {
            "n": len(rows),
            "elapsed_s": median_min_max([item["elapsed_s"] for item in rows]),
            "rss_end_delta_bytes": median_min_max([item["rss_end_delta_bytes"] for item in rows]),
            "rss_end_bytes": median_min_max([item["rss_end_bytes"] for item in rows]),
            "sampled_peak_rss_bytes": median_min_max(
                [item["sampled_peak_rss_bytes"] for item in rows if item["sampled_peak_rss_bytes"] is not None]
            )
            if any(item["sampled_peak_rss_bytes"] is not None for item in rows)
            else None,
            "ru_maxrss_bytes": median_min_max([item["ru_maxrss_bytes"] for item in rows]),
            "table_nbytes": median_min_max([item["table_nbytes"] for item in rows]),
            "sampled_rss_sample_count": {
                "median": statistics.median([item["sampled_rss_sample_count"] for item in rows]),
                "min": min(item["sampled_rss_sample_count"] for item in rows),
                "max": max(item["sampled_rss_sample_count"] for item in rows),
            },
        }
    return out


def write_trials_csv(path: Path, trials: list[dict]) -> None:
    fields = [
        "trial",
        "rep",
        "format",
        "elapsed_s",
        "rss_baseline_bytes",
        "rss_end_bytes",
        "rss_end_delta_bytes",
        "sampled_peak_rss_bytes",
        "sampled_rss_sample_count",
        "sample_interval_s",
        "ru_maxrss_bytes",
        "table_nbytes",
        "table_get_total_buffer_size",
        "num_rows",
        "file_bytes",
        "digest_s",
        "parent_wall_s",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in trials:
            writer.writerow(row)


def format_seconds(value: float) -> str:
    return "{:.4f}".format(value)


def format_mib(value_bytes: float) -> str:
    return "{:.2f}".format(bytes_mib(int(value_bytes)))


def print_summary_table(report: dict) -> None:
    files = report["files"]
    summary = report["summary"]
    print()
    print("热缓存全量读取（终点 = pyarrow.Table；单线程；计时不含 import / 预热 / 校验）")
    print(
        "缓存定义：每个 worker 启动前，父进程将该文件顺序读到 EOF，依赖操作系统页缓存。"
        "这是热读，不是磁盘冷读。"
    )
    print()
    headers = (
        "格式",
        "文件大小(MiB)",
        "中位耗时(s)",
        "最小(s)",
        "最大(s)",
        "结束RSS增量中位(MiB)",
        "Table.nbytes中位(MiB)",
    )
    rows = [headers]
    for fmt in FORMATS:
        stats = summary[fmt]
        rows.append(
            (
                fmt,
                format_mib(files[fmt]["bytes"]),
                format_seconds(stats["elapsed_s"]["median"]),
                format_seconds(stats["elapsed_s"]["min"]),
                format_seconds(stats["elapsed_s"]["max"]),
                format_mib(stats["rss_end_delta_bytes"]["median"]),
                format_mib(stats["table_nbytes"]["median"]),
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(headers))]
    for index, row in enumerate(rows):
        line = "  ".join(row[i].ljust(widths[i]) for i in range(len(headers)))
        print(line)
        if index == 0:
            print("  ".join("-" * widths[i] for i in range(len(headers))))
    print()
    print(
        "RSS 说明：结束 RSS 增量 = 仍持有 Table 时的 RSS − import 后基线，是最直接可解释的驻留增量。"
    )
    print(
        "sampled_peak 是父进程按约 {ms:.0f} ms 轮询 /proc 得到的峰值，可能漏掉短尖峰；"
        "Arrow 若只在几毫秒内完成，样本数可能很少，这时不要把 sampled_peak 当成精确峰值。".format(
            ms=report["limits"]["sample_interval_ms"]
        )
    )
    print(
        "ru_maxrss 是进程生命周期高水位（含 import 与分配器），不是从基线重新起算的读取峰值。"
    )
    print(
        "采样开销：轮询发生在父进程，不在 worker 计时路径里加线程或额外 syscall；"
        "对 worker 墙钟影响很小，但采样不是免费的，且无法保证捕到所有尖峰。"
    )
    print()
    validation = report["validation"]
    digest_ok = validation["digests_match"]
    strong = validation["strong_equals"]
    print(
        "内容校验：列聚合摘要一致={}; 独立进程逐批 equals={}（{} 行，{} 批）。".format(
            digest_ok,
            strong.get("passed"),
            strong.get("total_rows"),
            strong.get("batches_compared"),
        )
    )
    print("详细结果：{}".format(report["output"]["summary_json"]))
    print("各轮原始数据：{}".format(report["output"]["trials_csv"]))
    print("请把上面的表以及 summary.json 贴回来（保留原始数字，不要只凭感觉）。")


def parent_main(args: argparse.Namespace) -> int:
    data_dir = args.data_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if args.runs < MIN_RUNS:
        raise SystemExit("--runs 至少为 {}，当前为 {}".format(MIN_RUNS, args.runs))
    data = load_data_files(data_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    refuse_if_exists(output_dir)

    schedule = build_schedule(args.runs, args.seed)
    sample_interval_s = args.sample_interval_ms / 1000.0
    import psutil
    import pyarrow as pa

    print(
        "准备热缓存读取：data-dir={}；{} 种格式 × {} 次；输出目录：{}".format(
            data_dir, len(FORMATS), args.runs, output_dir
        ),
        flush=True,
    )
    trials = []
    tmp_dir = output_dir / ".benchmark-tmp"
    if tmp_dir.exists():
        import shutil

        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()
    try:
        for item in schedule:
            fmt = item["format"]
            info = data["files"][fmt]
            print(
                "  trial {trial}/{total}  {fmt}  rep={rep}  先预热再测量 ...".format(
                    trial=item["trial"],
                    total=len(schedule),
                    fmt=fmt,
                    rep=item["rep"],
                ),
                flush=True,
            )
            warmed = warm_cache(info["path"])
            if warmed != info["bytes"]:
                raise RuntimeError(
                    "预热读取字节数与文件大小不符：{} 读到 {}，文件 {}".format(
                        info["path"], warmed, info["bytes"]
                    )
                )
            result_file = tmp_dir / "{}-rep{}.json".format(fmt, item["rep"])
            payload = run_worker(
                fmt=fmt,
                path=info["path"],
                result_file=result_file,
                sample_interval_s=sample_interval_s,
            )
            trial = {
                "trial": item["trial"],
                "rep": item["rep"],
                "format": fmt,
                "file_name": info["name"],
                "file_bytes": info["bytes"],
                "warmed_bytes": warmed,
                **payload,
            }
            trials.append(trial)
            print(
                "    {fmt}  elapsed={elapsed:.4f}s  end_rss_delta={delta:.2f} MiB  "
                "table={table:.2f} MiB  samples={samples}".format(
                    fmt=fmt,
                    elapsed=payload["elapsed_s"],
                    delta=bytes_mib(payload["rss_end_delta_bytes"]),
                    table=bytes_mib(payload["table_nbytes"]),
                    samples=payload["sampled_rss_sample_count"],
                ),
                flush=True,
            )

        digest_groups = {}
        for trial in trials:
            key = json.dumps(trial["digest"], sort_keys=True, ensure_ascii=False)
            digest_groups.setdefault(key, []).append(
                {"trial": trial["trial"], "format": trial["format"], "rep": trial["rep"]}
            )
        digests_match = len(digest_groups) == 1

        validate_file = tmp_dir / "validation.json"
        print("  启动独立校验进程，逐批比较三种文件的逻辑内容 ...", flush=True)
        strong_equals = run_strong_validation(data_dir, validate_file)

        summary = summarize(trials)
        manifest = data["manifest"]
        report = {
            "environment": {
                "python": sys.version,
                "python_executable": sys.executable,
                "platform": platform.platform(),
                "pyarrow_version": pa.__version__,
                "psutil_version": psutil.__version__,
                "os_cpu_count": os.cpu_count(),
            },
            "limits": {
                "use_threads": False,
                "pyarrow_cpu_count": 1,
                "pyarrow_io_thread_count": 1,
                "parquet_memory_map": False,
                "parquet_pre_buffer": False,
                "arrow_ipc_source": "pa.OSFile (not mmap) + open_file().read_all()",
                "csv_explicit_schema": True,
                "sample_interval_ms": args.sample_interval_ms,
                "note": "各读取路径均显式 use_threads=False，避免默认线程数不同造成混淆。",
            },
            "cache_definition": (
                "warm：每个 worker 启动前，由父进程将该目标文件按 8MiB 块顺序读到 EOF，"
                "依赖操作系统页缓存。不做文件哈希。这不是磁盘冷读；"
                "若刚运行过 generate_data.py，文件本来就很可能已经在页缓存中。"
            ),
            "data_dir": str(data_dir),
            "manifest_path": str(data["manifest_path"]) if data["manifest_path"] else None,
            "manifest_rows": None if not manifest else manifest.get("rows"),
            "manifest_seed": None if not manifest else manifest.get("seed"),
            "writer_config": None if not manifest else manifest.get("writer_config"),
            "files": {
                fmt: {
                    "name": info["name"],
                    "bytes": info["bytes"],
                    "mib": bytes_mib(info["bytes"]),
                    "mib_definition": "1 MiB = 1024*1024 bytes",
                    "manifest_bytes": info.get("manifest_bytes"),
                }
                for fmt, info in data["files"].items()
            },
            "seed": args.seed,
            "runs_per_format": args.runs,
            "schedule": schedule,
            "trials": [
                {
                    key: value
                    for key, value in trial.items()
                    if key not in {"worker_stdout", "worker_stderr"}
                }
                for trial in trials
            ],
            "summary": summary,
            "validation": {
                "digests_match": digests_match,
                "digest_groups": len(digest_groups),
                "digest": trials[0]["digest"] if digests_match else None,
                "digest_mismatch": None
                if digests_match
                else {"groups": [{"digest_key": key, "trials": refs} for key, refs in digest_groups.items()]},
                "strong_equals": strong_equals,
            },
            "metric_notes": {
                "elapsed_s": "time.perf_counter 仅覆盖读取并物化成 Table；不含 import、父进程预热、摘要和 equals 校验。",
                "rss_baseline_bytes": "worker 在导入 pyarrow/csv/parquet/ipc/compute/psutil 并设置单线程之后、读取之前的 RSS。",
                "rss_end_bytes": "读取结束后、仍持有 Table 时的 RSS。",
                "rss_end_delta_bytes": "rss_end_bytes - rss_baseline_bytes，最直接可解释的驻留增量。",
                "sampled_peak_rss_bytes": (
                    "父进程在发出 GO 前后轮询子进程 RSS 得到的最大值；间隔约 {} ms，"
                    "可能漏掉短尖峰，样本少时误差更明显。不是硬件级峰值。"
                ).format(args.sample_interval_ms),
                "ru_maxrss_bytes": (
                    "Linux getrusage(RUSAGE_SELF).ru_maxrss（按 KB×1024 换算）。"
                    "这是进程生命周期高水位，包含 import 与分配器保留，"
                    "不是把基线清零之后的读取峰值。"
                ),
                "table_nbytes": "pyarrow.Table.nbytes，是表内 buffer 的逻辑字节，不是整个进程 RSS。",
                "rss_sampler_cost": (
                    "采样由父进程读 /proc，不在 worker 计时路径中加线程。"
                    "对 worker 墙钟影响很小，但采样本身有父进程轮询开销，也不能保证捕到所有尖峰。"
                ),
            },
            "output": {
                "summary_json": str(output_dir / RESULT_JSON),
                "trials_csv": str(output_dir / RESULT_CSV),
            },
        }
        if not digests_match or not strong_equals.get("passed"):
            (output_dir / RESULT_JSON).write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            write_trials_csv(output_dir / RESULT_CSV, trials)
            raise SystemExit(
                "内容校验失败。digests_match={}，strong_equals={}。结果已写入 {}。".format(
                    digests_match,
                    strong_equals.get("passed"),
                    output_dir / RESULT_JSON,
                )
            )
        refuse_if_exists(output_dir)
        (output_dir / RESULT_JSON).write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        write_trials_csv(output_dir / RESULT_CSV, trials)
    finally:
        import shutil

        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)

    final_report = json.loads((output_dir / RESULT_JSON).read_text(encoding="utf-8"))
    print_summary_table(final_report)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="在热缓存下把 CSV / Parquet / Arrow IPC 全量读成 pyarrow.Table，并记录耗时与内存。"
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--validate", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--format", choices=FORMATS, help=argparse.SUPPRESS)
    parser.add_argument("--input", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result-file", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="generate_data.py 的输出目录，需包含 orders.csv / orders.parquet / orders.arrow",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="结果目录，默认 results/warm-时间戳；已有结果时拒绝覆盖，可指定 ./results/warm-01",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help="每种格式的测量次数，至少 {}，默认 {}".format(MIN_RUNS, DEFAULT_RUNS),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="打乱每轮格式顺序的随机种子，默认 {}".format(DEFAULT_SEED),
    )
    parser.add_argument(
        "--sample-interval-ms",
        type=float,
        default=DEFAULT_SAMPLE_INTERVAL_MS,
        help="父进程采样子进程 RSS 的间隔（毫秒），默认 {}".format(DEFAULT_SAMPLE_INTERVAL_MS),
    )
    args = parser.parse_args(argv)
    if args.worker:
        if not args.format or not args.input or not args.result_file:
            parser.error("内部 --worker 需要 --format、--input、--result-file")
        return args
    if args.validate:
        if not args.data_dir or not args.result_file:
            parser.error("内部 --validate 需要 --data-dir、--result-file")
        return args
    if not args.data_dir:
        parser.error("请提供 --data-dir")
    if args.output_dir is None:
        args.output_dir = default_output_dir()
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.worker:
        return worker_main(args)
    if args.validate:
        return validate_main(args)
    return parent_main(args)


if __name__ == "__main__":
    raise SystemExit(main())
