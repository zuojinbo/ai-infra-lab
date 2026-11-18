#!/usr/bin/env python3
"""热缓存下，比较同一张表写出的一对 Parquet：compression=NONE 与 ZSTD level 3。

独立脚本，不 import 也不修改 v1 / v2 / projection / generate_data。
依赖：Python 标准库 + pyarrow + psutil（不要额外安装 numpy 或 pandas）。
目标环境：Python 3.10、PyArrow 25。启动时检查 ZSTD 可用，不可用则明确退出。

设计要点：
- 统一源是 --data-dir 中已有的 orders.arrow（只读，不改原始 data 目录里的任何文件）。
  不用原始 orders.parquet 作为本轮计时对照：那份文件的写入参数未在本脚本里再次核验。
- 父进程在计时之外加载源表，再用同一 pa.Table、同一组显式写入参数写出一对新文件；
  唯一差别是预定的压缩配置（NONE 无 level，或 ZSTD level=3）。写入后检查行组、
  列编码和 codec；编码或行组不一致则拒绝称作“只有压缩不同”。
- 测量协议对齐 benchmark_read_v2：全列 pa.Table、每次独立 worker、已知惰性模块
  在 RSS 基线之前导入、热缓存按字节读到 EOF、非 mmap、单线程、默认 5 次、
  seed 42 下每轮两个变体随机交错、READY/GO/READ_DONE/SAMPLED/摘要/DONE。
  计时不含源表加载、写入和校验。sampled_peak 只覆盖读窗口。ru_maxrss 是
  进程生命周期高水位。
- 校验在独立进程中进行，不污染 worker 内存：各轮逻辑摘要须与源表一致，
  并按固定行数分批对源表做 table.equals。
- 不测量、也不声称实际 IO 请求量或 CPU 分解。这是热缓存全列读取。
- 结果目录或原始数据已存在目标文件时拒绝覆盖。
"""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import importlib
import importlib.util
import json
import os
import platform
import queue
import random
import select
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

MIB = 1024 * 1024
SCRIPT_VERSION = "compression-v1"
DEFAULT_RUNS = 5
MIN_RUNS = 3
DEFAULT_SEED = 42
DEFAULT_SAMPLE_INTERVAL_MS = 2
VALIDATE_BATCH_SIZE = 100_000
WORKER_TIMEOUT_S = 600
HANDSHAKE_TIMEOUT_S = 60
ROW_GROUP_SIZE = 100_000
ZSTD_LEVEL = 3
SOURCE_IPC_NAME = "orders.arrow"
ORIGINAL_PARQUET_NAME = "orders.parquet"
MANIFEST_NAME = "manifest.json"
DERIVED_MANIFEST_NAME = "derived_manifest.json"
RESULT_JSON = "summary.json"
RESULT_CSV = "trials.csv"

# pq.read_table() 会在 ParquetDataset.__init__ 里惰性 import 这些模块。
# 提前导入只为把已知模块加载移出计时区；不打开数据文件，不预分配 Table。
KNOWN_LAZY_MODULES = (
    "pyarrow.dataset",
    "pyarrow._dataset",
    "pyarrow._dataset_parquet",
    "pyarrow._dataset_parquet_encryption",
    "pyarrow._dataset_orc",
    "pyarrow.fs",
)

# 与 generate_data.py 保持一致，便于读回校验。
SCHEMA_FIELDS = [
    ("order_id", "int64"),
    ("user_id", "int64"),
    ("amount_cents", "int64"),
    ("city", "string"),
    ("event_time", "int64"),
    ("category", "string"),
]

# 两个变体共用的显式写入参数。只允许压缩配置不同。
# 取值对齐 generate_data.py 已写明的项，以及 PyArrow 25 的 write_table 默认值。
SHARED_WRITE_KWARGS = {
    "row_group_size": ROW_GROUP_SIZE,
    "use_dictionary": True,
    "version": "2.6",
    "write_statistics": True,
    "use_byte_stream_split": False,
    "column_encoding": None,
    "data_page_version": "1.0",
    "use_compliant_nested_type": True,
    "store_schema": True,
    "write_page_index": False,
    "write_page_checksum": False,
    "data_page_size": None,
    "write_batch_size": None,
    "dictionary_pagesize_limit": None,
    "flavor": None,
    "use_deprecated_int96_timestamps": None,
    "coerce_timestamps": None,
    "allow_truncated_timestamps": False,
    "store_decimal_as_integer": False,
}

# compression / compression_level 是本轮唯一预定差异。
# NONE 不支持 compression_level，显式传 None；ZSTD 使用 level 3（属于该压缩配置，不是额外变量）。
VARIANTS = {
    "parquet_none": {
        "key": "parquet_none",
        "label": "parquet_none",
        "filename": "orders_none.parquet",
        "compression": "NONE",
        "compression_level": None,
        "expected_metadata_codec": "UNCOMPRESSED",
    },
    "parquet_zstd": {
        "key": "parquet_zstd",
        "label": "parquet_zstd",
        "filename": "orders_zstd_level3.parquet",
        "compression": "ZSTD",
        "compression_level": ZSTD_LEVEL,
        "expected_metadata_codec": "ZSTD",
    },
}
VARIANT_KEYS = ("parquet_none", "parquet_zstd")


def schema():
    import pyarrow as pa

    return pa.schema([pa.field(name, getattr(pa, typ)()) for name, typ in SCHEMA_FIELDS])


def bytes_mib(n: int) -> float:
    return n / MIB


def median_min_max(values: list[float]) -> dict:
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def readline_timeout(stream, timeout: float):
    """带超时地读一行。超时返回 None，避免握手死等。"""
    ready, _, _ = select.select([stream], [], [], timeout)
    if not ready:
        return None
    return stream.readline()


def pandas_status() -> dict:
    return {
        "pandas_installed": importlib.util.find_spec("pandas") is not None,
        "pandas_in_sys_modules": "pandas" in sys.modules,
    }


def require_zstd(level: int = ZSTD_LEVEL) -> dict:
    """确认当前 PyArrow 构建启用了 ZSTD，且预定 level 在支持范围内。"""
    try:
        import pyarrow as pa
    except ImportError as exc:
        raise SystemExit(
            "未找到 pyarrow。本脚本只依赖标准库 + pyarrow + psutil。\n{}".format(exc)
        ) from exc

    if not hasattr(pa, "Codec") or not pa.Codec.is_available("zstd"):
        raise SystemExit(
            "当前 PyArrow（{}）未启用 ZSTD：pa.Codec.is_available('zstd') 为假。"
            "本实验需要可用的 ZSTD 才能写出对照文件。请更换带 zstd 的 PyArrow 构建后再运行。".format(
                getattr(pa, "__version__", "未知")
            )
        )
    minimum = int(pa.Codec.minimum_compression_level("zstd"))
    maximum = int(pa.Codec.maximum_compression_level("zstd"))
    default = int(pa.Codec.default_compression_level("zstd"))
    if not (minimum <= level <= maximum):
        raise SystemExit(
            "预定 ZSTD level {} 不在该构建支持范围 [{}, {}]。".format(level, minimum, maximum)
        )
    # 实际构造一次，避免只检查 is_available 却在该 level 上构造失败。
    codec = pa.Codec("zstd", level)
    return {
        "available": True,
        "name": codec.name,
        "requested_level": level,
        "actual_level": int(codec.compression_level) if codec.compression_level is not None else None,
        "default_level": default,
        "minimum_level": minimum,
        "maximum_level": maximum,
        "pyarrow_version": pa.__version__,
        "note": "ZSTD level 3 是本轮预定压缩配置的一部分，不是在 codec 之外另加的变量。",
    }


def preload_known_read_modules() -> dict:
    """导入 pq.read_table 路径上已定位的惰性模块，不读取任何数据文件。"""
    imported = []
    missing = []
    for name in KNOWN_LAZY_MODULES:
        try:
            importlib.import_module(name)
            imported.append(name)
        except ImportError as exc:
            missing.append({"name": name, "error": str(exc)})
    touched = []
    import pyarrow.dataset as ds

    if hasattr(ds, "ParquetFileFormat"):
        ds.ParquetFileFormat()
        touched.append("pyarrow.dataset.ParquetFileFormat()")
    if hasattr(ds, "IpcFileFormat"):
        ds.IpcFileFormat()
        touched.append("pyarrow.dataset.IpcFileFormat()")
    if hasattr(ds, "CsvFileFormat"):
        ds.CsvFileFormat()
        touched.append("pyarrow.dataset.CsvFileFormat()")
    status = pandas_status()
    return {
        "known_lazy_modules": list(KNOWN_LAZY_MODULES),
        "imported": imported,
        "missing": missing,
        "touched_format_objects": touched,
        "pandas_installed": status["pandas_installed"],
        "pandas_in_sys_modules": status["pandas_in_sys_modules"],
        "note": (
            "只把已定位的惰性模块加载移到基线和计时之前；"
            "不预读数据，也不声称消除所有首次初始化。"
        ),
    }


def start_line_reader(stream) -> queue.Queue:
    lines: queue.Queue = queue.Queue()

    def _run() -> None:
        try:
            for line in stream:
                lines.put(line)
        finally:
            lines.put(None)

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return lines


def wait_queue_line(lines: queue.Queue, expected: str, timeout: float, proc, label: str) -> str:
    deadline = time.time() + timeout
    extras = []
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise RuntimeError(
                "等待 {} 超时（{} 秒）。已收到：{}。进程退出码：{}".format(
                    label, timeout, extras, proc.poll() if proc is not None else "n/a"
                )
            )
        try:
            line = lines.get(timeout=remaining)
        except queue.Empty:
            continue
        if line is None:
            raise RuntimeError(
                "等待 {} 时 stdout 已关闭。已收到：{}。进程退出码：{}".format(
                    label, extras, proc.poll() if proc is not None else "n/a"
                )
            )
        if line.strip() == expected:
            return line
        extras.append(line.strip())


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


def read_parquet_table(path: Path):
    import pyarrow.parquet as pq

    return pq.read_table(
        path,
        memory_map=False,
        use_threads=False,
        pre_buffer=False,
    )


def load_source_table(path: Path):
    """只读加载 orders.arrow。用于父进程写出对照文件，不计时。"""
    import pyarrow as pa
    import pyarrow.ipc as paipc

    options = paipc.IpcReadOptions(use_threads=False)
    with pa.OSFile(str(path), "rb") as source:
        reader = paipc.open_file(source, options=options)
        table = reader.read_all()
    expected = schema()
    if not table.schema.equals(expected, check_metadata=False):
        raise RuntimeError(
            "源表 schema 与约定不一致：实际 {}，期望 {}".format(table.schema, expected)
        )
    if table.schema.names != [name for name, _ in SCHEMA_FIELDS]:
        raise RuntimeError("源表列次序与约定不一致：{}".format(table.schema.names))
    return table


def iter_source_batches(kind: str, path: Path):
    import pyarrow as pa
    import pyarrow.ipc as paipc
    import pyarrow.parquet as pq

    if kind == "arrow_ipc":
        options = paipc.IpcReadOptions(use_threads=False)
        with pa.OSFile(str(path), "rb") as source:
            reader = paipc.open_file(source, options=options)
            for index in range(reader.num_record_batches):
                yield reader.get_batch(index)
        return
    if kind == "parquet":
        parquet_file = pq.ParquetFile(path, memory_map=False)
        for batch in parquet_file.iter_batches(batch_size=VALIDATE_BATCH_SIZE, use_threads=False):
            yield batch
        return
    raise ValueError("未知来源：{}".format(kind))


def iter_fixed_tables(kind: str, path: Path, batch_size: int):
    """把任意来源切成固定行数的 Table，再做 equals。"""
    import pyarrow as pa

    expected = schema()
    pending = []
    pending_rows = 0
    for batch in iter_source_batches(kind, path):
        if batch.num_rows == 0:
            continue
        pending.append(batch)
        pending_rows += batch.num_rows
        while pending_rows >= batch_size:
            table = pa.Table.from_batches(pending, schema=expected)
            yield table.slice(0, batch_size)
            rest = table.slice(batch_size)
            pending = rest.to_batches() if rest.num_rows else []
            pending_rows = rest.num_rows
    if pending_rows:
        yield pa.Table.from_batches(pending, schema=expected)


def linux_ru_maxrss_bytes() -> dict:
    import resource

    usage = resource.getrusage(resource.RUSAGE_SELF)
    raw = int(usage.ru_maxrss)
    return {
        "ru_maxrss_raw": raw,
        "ru_maxrss_unit": "kilobytes_on_linux",
        "ru_maxrss_bytes": raw * 1024,
    }


def inspect_parquet(path: Path) -> dict:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(path, memory_map=False)
    metadata = parquet_file.metadata
    schema_names = list(parquet_file.schema_arrow.names)
    row_groups = []
    for i in range(metadata.num_row_groups):
        group = metadata.row_group(i)
        columns = []
        for j in range(group.num_columns):
            column = group.column(j)
            columns.append(
                {
                    "name": column.path_in_schema,
                    "physical_type": str(column.physical_type),
                    "compression": column.compression,
                    "encodings": list(column.encodings),
                    "total_compressed_size": int(column.total_compressed_size),
                    "total_uncompressed_size": int(column.total_uncompressed_size),
                    "num_values": int(column.num_values),
                }
            )
        row_groups.append({"num_rows": int(group.num_rows), "columns": columns})
    codecs = sorted(
        {
            column["compression"]
            for group in row_groups
            for column in group["columns"]
        }
    )
    return {
        "num_rows": int(metadata.num_rows),
        "num_row_groups": int(metadata.num_row_groups),
        "row_group_num_rows": [item["num_rows"] for item in row_groups],
        "schema_names": schema_names,
        "created_by": metadata.created_by,
        "serialized_size": int(metadata.serialized_size) if metadata.serialized_size is not None else None,
        "unique_compressions": codecs,
        "row_groups": row_groups,
    }


def layout_fingerprint(info: dict) -> dict:
    """行组结构与列编码（不含 codec 和压缩后大小），用于核验“唯一变量”。"""
    return {
        "num_rows": info["num_rows"],
        "num_row_groups": info["num_row_groups"],
        "row_group_num_rows": info["row_group_num_rows"],
        "schema_names": info["schema_names"],
        "encodings": [
            [{"name": column["name"], "encodings": column["encodings"]} for column in group["columns"]]
            for group in info["row_groups"]
        ],
    }


def compare_pair_metadata(none_info: dict, zstd_info: dict) -> dict:
    none_layout = layout_fingerprint(none_info)
    zstd_layout = layout_fingerprint(zstd_info)
    layout_equal = none_layout == zstd_layout
    uncompressed_mismatches = []
    for rg_index, (none_rg, zstd_rg) in enumerate(zip(none_info["row_groups"], zstd_info["row_groups"])):
        for none_col, zstd_col in zip(none_rg["columns"], zstd_rg["columns"]):
            if none_col["name"] != zstd_col["name"]:
                continue
            if none_col["total_uncompressed_size"] != zstd_col["total_uncompressed_size"]:
                uncompressed_mismatches.append(
                    {
                        "row_group": rg_index,
                        "name": none_col["name"],
                        "none": none_col["total_uncompressed_size"],
                        "zstd": zstd_col["total_uncompressed_size"],
                    }
                )
    none_codecs = none_info["unique_compressions"]
    zstd_codecs = zstd_info["unique_compressions"]
    codec_ok = none_codecs == ["UNCOMPRESSED"] and zstd_codecs == ["ZSTD"]
    return {
        "layout_equal": layout_equal,
        "codec_as_expected": codec_ok,
        "none_compressions": none_codecs,
        "zstd_compressions": zstd_codecs,
        "uncompressed_size_mismatches": uncompressed_mismatches,
        "unique_variable_ok": layout_equal and codec_ok,
        "note": (
            "unique_variable_ok 表示两份新文件的行组划分、schema 列序和各列 encodings 相同，"
            "且 codec 分别为 UNCOMPRESSED 与 ZSTD。"
            "压缩后大小必然不同；未压缩大小若略有差别会记录，但不单独作为失败条件。"
        ),
    }


def write_one_parquet(table, path: Path, variant: dict) -> dict:
    import pyarrow.parquet as pq

    started = time.perf_counter()
    pq.write_table(
        table,
        path,
        compression=variant["compression"],
        compression_level=variant["compression_level"],
        **SHARED_WRITE_KWARGS,
    )
    write_s = time.perf_counter() - started
    return {
        "bytes": path.stat().st_size,
        "sha256": file_sha256(path),
        "write_wall_s_informational": write_s,
        "write_timing_note": "仅作记录，不是本实验的测量指标，也不进入 worker 计时。",
    }


def worker_main(args: argparse.Namespace) -> int:
    import psutil
    import pyarrow as pa
    import pyarrow.compute as pc  # noqa: F401
    import pyarrow.csv as pacsv  # noqa: F401
    import pyarrow.ipc as paipc  # noqa: F401
    import pyarrow.parquet as pq  # noqa: F401

    thread_info = configure_single_thread()
    preload_info = preload_known_read_modules()
    process = psutil.Process()
    rss_baseline = int(process.memory_info().rss)

    sys.stdout.write("READY\n")
    sys.stdout.flush()
    go = readline_timeout(sys.stdin, HANDSHAKE_TIMEOUT_S)
    if go is None:
        raise SystemExit("worker 等待 GO 超时")
    if go.strip() != "GO":
        raise SystemExit("worker 未收到 GO，实际收到：{!r}".format(go))

    started = time.perf_counter()
    table = read_parquet_table(Path(args.input))
    num_rows = int(table.num_rows)
    table_nbytes = int(table.nbytes)
    buffer_size = int(table.get_total_buffer_size()) if hasattr(table, "get_total_buffer_size") else None
    elapsed_s = time.perf_counter() - started

    rss_end = int(process.memory_info().rss)
    ru_after_read = linux_ru_maxrss_bytes()

    sys.stdout.write("READ_DONE\n")
    sys.stdout.flush()
    sampled = readline_timeout(sys.stdin, HANDSHAKE_TIMEOUT_S)
    if sampled is None:
        raise SystemExit("worker 等待 SAMPLED 超时；已持有 Table 但未开始摘要")
    if sampled.strip() != "SAMPLED":
        raise SystemExit("worker 未收到 SAMPLED，实际收到：{!r}".format(sampled))

    digest_started = time.perf_counter()
    digest = logical_digest(table)
    digest_s = time.perf_counter() - digest_started
    ru_at_exit_point = linux_ru_maxrss_bytes()

    expected = schema()
    if not table.schema.equals(expected, check_metadata=False):
        raise RuntimeError("读回 schema 与约定不一致：{}".format(table.schema))

    result = {
        "script_version": SCRIPT_VERSION,
        "variant": args.variant,
        "format": "parquet",
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
        "preload": preload_info,
        "sampling_window": "read_only_before_digest",
        "pandas_installed": preload_info["pandas_installed"],
        "pandas_in_sys_modules": pandas_status()["pandas_in_sys_modules"],
        "digest": digest,
        "pyarrow_version": pa.__version__,
        "psutil_version": __import__("psutil").__version__,
        "ru_maxrss_raw": ru_after_read["ru_maxrss_raw"],
        "ru_maxrss_unit": ru_after_read["ru_maxrss_unit"],
        "ru_maxrss_bytes": ru_after_read["ru_maxrss_bytes"],
        "ru_maxrss_after_digest_bytes": ru_at_exit_point["ru_maxrss_bytes"],
        "ru_maxrss_meaning": "process_lifetime_high_water_mark_not_reset_at_baseline",
    }
    Path(args.result_file).write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
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


def derived_artifact_paths(output_dir: Path) -> list[Path]:
    names = [variant["filename"] for variant in VARIANTS.values()]
    names.extend([DERIVED_MANIFEST_NAME, RESULT_JSON, RESULT_CSV])
    return [output_dir / name for name in names]


def result_only_paths(output_dir: Path) -> list[Path]:
    """只包含最终测量结果，不含已经写出的派生 Parquet。"""
    return [output_dir / RESULT_JSON, output_dir / RESULT_CSV]


def refuse_if_exists(output_dir: Path, paths: list[Path] | None = None) -> None:
    existing = [path for path in (paths or derived_artifact_paths(output_dir)) if path.exists()]
    if not existing:
        return
    lines = [
        "结果或派生 Parquet 已存在，拒绝覆盖，以免毁掉上次实验记录或原始数据：",
        *["  - {}".format(path) for path in existing],
        "请更换 --output-dir，例如 ./results/warm-04-zstd。",
    ]
    raise SystemExit("\n".join(lines))


def default_output_dir() -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return Path("results") / "warm-zstd-{}".format(stamp)


def find_source(data_dir: Path) -> dict:
    path = data_dir / SOURCE_IPC_NAME
    if not path.exists():
        raise SystemExit(
            "在 {} 中找不到统一源文件 {}。本实验用已有 Arrow IPC 作为同一张表的来源，"
            "然后在输出目录新写一对 Parquet；不会改写 data 目录。".format(data_dir, SOURCE_IPC_NAME)
        )
    manifest_path = data_dir / MANIFEST_NAME
    manifest = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        recorded = manifest.get("files", {}).get(SOURCE_IPC_NAME, {}).get("bytes")
        actual = path.stat().st_size
        if recorded is not None and recorded != actual:
            raise SystemExit(
                "源文件大小与 manifest 不一致：{} 实际 {} 字节，manifest 记录 {} 字节。".format(
                    path, actual, recorded
                )
            )
    return {
        "kind": "arrow_ipc",
        "name": SOURCE_IPC_NAME,
        "path": path,
        "bytes": path.stat().st_size,
        "manifest": manifest,
        "manifest_path": manifest_path if manifest is not None else None,
    }


def build_schedule(n_runs: int, seed: int) -> list[dict]:
    rng = random.Random(seed)
    schedule = []
    trial = 0
    for rep in range(1, n_runs + 1):
        order = list(VARIANT_KEYS)
        rng.shuffle(order)
        for key in order:
            trial += 1
            schedule.append({"trial": trial, "rep": rep, "variant": key})
    return schedule


def run_worker(
    *,
    variant: str,
    path: Path,
    result_file: Path,
    sample_interval_s: float,
) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--variant",
        variant,
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
    stdout_q = start_line_reader(proc.stdout)
    stderr_q = start_line_reader(proc.stderr)
    samples = []
    sample_error = None
    stdout_extra = []
    stderr_lines = []

    def collect_stderr() -> None:
        while True:
            try:
                line = stderr_q.get_nowait()
            except queue.Empty:
                return
            if line is None:
                return
            stderr_lines.append(line)

    def fail_dump() -> str:
        collect_stderr()
        return "".join(stderr_lines)

    try:
        wait_queue_line(stdout_q, "READY", HANDSHAKE_TIMEOUT_S, proc, "READY")
        try:
            samples.append(int(child.memory_info().rss))
        except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
            sample_error = str(exc)
        proc.stdin.write("GO\n")
        proc.stdin.flush()
        deadline = time.time() + WORKER_TIMEOUT_S
        got_read_done = False
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                proc.kill()
                raise RuntimeError(
                    "worker 等待 READ_DONE 超时（{} 秒）：{}".format(WORKER_TIMEOUT_S, variant)
                )
            try:
                line = stdout_q.get(timeout=min(sample_interval_s, remaining))
            except queue.Empty:
                try:
                    samples.append(int(child.memory_info().rss))
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    collect_stderr()
                    raise RuntimeError(
                        "采样期间 worker 已退出，未收到 READ_DONE。stderr={}".format(fail_dump())
                    )
                if proc.poll() is not None:
                    raise RuntimeError(
                        "worker 在 READ_DONE 之前退出，退出码 {}。stderr={}".format(
                            proc.returncode, fail_dump()
                        )
                    )
                continue
            if line is None:
                raise RuntimeError(
                    "worker stdout 在 READ_DONE 之前关闭，退出码 {}。stderr={}".format(
                        proc.poll(), fail_dump()
                    )
                )
            if line.strip() == "READ_DONE":
                try:
                    samples.append(int(child.memory_info().rss))
                except (psutil.NoSuchProcess, psutil.AccessDenied) as exc:
                    sample_error = str(exc)
                got_read_done = True
                break
            stdout_extra.append(line.strip())
        if not got_read_done:
            raise RuntimeError("未收到 READ_DONE。stderr={}".format(fail_dump()))
        proc.stdin.write("SAMPLED\n")
        proc.stdin.flush()
        wait_queue_line(stdout_q, "DONE", HANDSHAKE_TIMEOUT_S, proc, "DONE")
        try:
            proc.stdin.close()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
            raise RuntimeError("worker 在 DONE 之后未退出。stderr={}".format(fail_dump()))
    except Exception:
        collect_stderr()
        if proc.poll() is None:
            proc.kill()
            try:
                proc.wait(timeout=10)
            except Exception:
                pass
        raise
    wall_s = time.perf_counter() - started
    collect_stderr()
    stderr_text = "".join(stderr_lines).strip()
    stdout_text = "\n".join(item for item in stdout_extra if item)
    if proc.returncode != 0:
        raise RuntimeError(
            "worker 退出码 {}。\nstdout={}\nstderr={}".format(
                proc.returncode, stdout_text, stderr_text
            )
        )
    if not result_file.exists():
        raise RuntimeError("worker 未写出结果文件。stderr={}".format(stderr_text))
    payload = json.loads(result_file.read_text(encoding="utf-8"))
    payload["sampled_peak_rss_bytes"] = max(samples) if samples else None
    payload["sampled_rss_sample_count"] = len(samples)
    payload["sample_interval_s"] = sample_interval_s
    payload["sample_error"] = sample_error
    payload["sampling_window"] = "read_only_before_digest"
    payload["parent_wall_s"] = wall_s
    payload["worker_stdout"] = stdout_text
    payload["worker_stderr"] = stderr_text
    return payload


def run_strong_validation(
    *,
    source_path: Path,
    none_path: Path,
    zstd_path: Path,
    result_file: Path,
) -> dict:
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--validate",
        "--source",
        str(source_path),
        "--parquet-none",
        str(none_path),
        "--parquet-zstd",
        str(zstd_path),
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
    source_path = Path(args.source).expanduser().resolve()
    none_path = Path(args.parquet_none).expanduser().resolve()
    zstd_path = Path(args.parquet_zstd).expanduser().resolve()
    batch_size = VALIDATE_BATCH_SIZE
    iterators = {
        "source": iter_fixed_tables("arrow_ipc", source_path, batch_size),
        "parquet_none": iter_fixed_tables("parquet", none_path, batch_size),
        "parquet_zstd": iter_fixed_tables("parquet", zstd_path, batch_size),
    }
    compared = 0
    total_rows = 0
    while True:
        batches = {}
        ended = []
        for key, iterator in iterators.items():
            try:
                batches[key] = next(iterator)
            except StopIteration:
                ended.append(key)
        if ended:
            if len(ended) != len(iterators):
                raise RuntimeError(
                    "源表与两个 Parquet 变体的行数不一致：先结束的是 {}，本批已读到的是 {}。".format(
                        ended, list(batches)
                    )
                )
            break
        rows = {key: table.num_rows for key, table in batches.items()}
        if len(set(rows.values())) != 1:
            raise RuntimeError("切齐后仍出现不同行数：{}".format(rows))
        reference = batches["source"]
        for key, table in batches.items():
            if key == "source":
                continue
            if not table.equals(reference):
                raise RuntimeError(
                    "第 {} 批（累计已比较 {} 行之后）{} 与源表的逻辑内容不一致。".format(
                        compared + 1, total_rows, key
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
        "pairs": ["parquet_none==source", "parquet_zstd==source"],
        "note": (
            "在独立进程中按固定行数切批后做 table.equals；"
            "对照对象是本轮加载的源表（orders.arrow），不是原始 data/orders.parquet。"
            "读路径与测量相同口径（IPC 用 OSFile，Parquet 非 mmap、use_threads=False）。"
        ),
    }
    Path(args.result_file).write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return 0


def summarize(trials: list[dict]) -> dict:
    out = {}
    for key in VARIANT_KEYS:
        rows = [item for item in trials if item["variant"] == key]
        out[key] = {
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
        "variant",
        "compression",
        "compression_level",
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
        "script_version",
        "sampling_window",
        "pandas_installed",
        "ru_maxrss_after_digest_bytes",
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
    print(
        "热缓存 Parquet 压缩对照 compression-v1"
        "（NONE vs ZSTD level 3；终点 = 全列 pyarrow.Table；单线程；计时不含加载/写入/import/预热/校验）"
    )
    print(
        "缓存定义：每个 worker 启动前，父进程将该目标文件顺序读到 EOF，依赖操作系统页缓存。"
        "这是热读，不是磁盘冷读。"
    )
    print(
        "唯一变量：两份新 Parquet 由同一源表、同一组显式写入参数生成，"
        "只改预定压缩配置（NONE / ZSTD level 3）。原始 data/orders.parquet 不是本轮计时基线。"
    )
    print(
        "假设请先保留：热读下 ZSTD 可能因解压而更慢，也可能因文件更小而持平或更快。"
        "本实验不测量 IO 请求量，也不做 CPU 分解，不能事先承诺谁更快。"
        "以本次实测中位数为准，同时保留最大值（含可能的首次偏慢）。"
    )
    print()
    headers = (
        "变体",
        "文件大小(MiB)",
        "中位耗时(s)",
        "最小(s)",
        "最大(s)",
        "结束RSS增量中位(MiB)",
        "Table.nbytes中位(MiB)",
    )
    rows = [headers]
    for key in VARIANT_KEYS:
        stats = summary[key]
        rows.append(
            (
                key,
                format_mib(files[key]["bytes"]),
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
        "Table.nbytes 是逻辑引用范围，不是进程 RSS。"
    )
    print(
        "sampled_peak 只覆盖读窗口，由父进程按约 {ms:.0f} ms 轮询 /proc；可能漏掉短尖峰。"
        "短读取的样本数可能很少，不要把它当成精确峰值。".format(
            ms=report["limits"]["sample_interval_ms"]
        )
    )
    print(
        "ru_maxrss 始终是进程生命周期高水位（含 import 与分配器），"
        "不是从基线重新起算的读取峰值，也不等于 sampled_peak。"
    )
    print(
        "不要把耗时差异全部解释为少读了磁盘：本实验是热缓存，也没有 IO 请求量证据。"
    )
    print()
    validation = report["validation"]
    digest_ok = validation["digests_match_source"]
    strong = validation["strong_equals"]
    meta = report["derived"]["metadata_comparison"]
    print(
        "内容校验：各轮摘要与源表一致={}; 独立进程逐批 equals={}（{} 行，{} 批）。".format(
            digest_ok,
            strong.get("passed"),
            strong.get("total_rows"),
            strong.get("batches_compared"),
        )
    )
    print(
        "写入核验：行组/列编码相同且 codec 符合预定配置={}；"
        "未压缩大小不一致的列数={}。".format(
            meta.get("unique_variable_ok"),
            len(meta.get("uncompressed_size_mismatches") or []),
        )
    )
    print("详细结果：{}".format(report["output"]["summary_json"]))
    print("各轮原始数据：{}".format(report["output"]["trials_csv"]))
    print("派生文件与写入配置：{}".format(report["output"]["derived_manifest"]))
    print("请把上面的表以及 summary.json 贴回来（保留原始数字）。本机试跑数字不是你的实验结果。")


def maybe_inspect_original_parquet(data_dir: Path, none_info: dict) -> dict | None:
    path = data_dir / ORIGINAL_PARQUET_NAME
    if not path.exists():
        return None
    info = inspect_parquet(path)
    original_layout = layout_fingerprint(info)
    none_layout = layout_fingerprint(none_info)
    return {
        "path": str(path),
        "bytes": path.stat().st_size,
        "used_as_timing_baseline": False,
        "metadata": info,
        "layout_matches_new_none": original_layout == none_layout,
        "note": (
            "仅作编码/行组参考，不是本轮计时对照。"
            "即使布局恰好与新 NONE 相同，本轮唯一变量仍只在两份新文件之间成立，"
            "因为原始文件并未由本脚本用同一组显式参数当场核验后写出。"
        ),
    }


def prepare_derived_pair(data_dir: Path, output_dir: Path, zstd_info: dict) -> dict:
    """在父进程加载源表并写出一对新 Parquet，与 worker 内存隔离，且不计时。"""
    import pyarrow as pa

    source = find_source(data_dir)
    print(
        "  加载统一源 {}（{} 字节）...".format(source["path"], source["bytes"]),
        flush=True,
    )
    table = load_source_table(source["path"])
    source_digest = logical_digest(table)
    source_rows = int(table.num_rows)
    source_schema = [field.name + ": " + str(field.type) for field in table.schema]
    source_sha = file_sha256(source["path"])

    derived = {}
    for key in VARIANT_KEYS:
        variant = VARIANTS[key]
        path = output_dir / variant["filename"]
        if path.exists():
            raise SystemExit("拒绝覆盖已有派生文件：{}".format(path))
        print(
            "  写出 {}（compression={}, level={}）...".format(
                path.name, variant["compression"], variant["compression_level"]
            ),
            flush=True,
        )
        written = write_one_parquet(table, path, variant)
        meta = inspect_parquet(path)
        if meta["num_rows"] != source_rows:
            raise RuntimeError(
                "{} 行数与源表不符：{} vs {}".format(path, meta["num_rows"], source_rows)
            )
        if meta["schema_names"] != [name for name, _ in SCHEMA_FIELDS]:
            raise RuntimeError("{} 列次序与源表不符：{}".format(path, meta["schema_names"]))
        if meta["unique_compressions"] != [variant["expected_metadata_codec"]]:
            raise RuntimeError(
                "{} 的 metadata codec 不是 {}，实际为 {}".format(
                    path, variant["expected_metadata_codec"], meta["unique_compressions"]
                )
            )
        derived[key] = {
            "variant": key,
            "name": variant["filename"],
            "path": path,
            "bytes": written["bytes"],
            "sha256": written["sha256"],
            "compression": variant["compression"],
            "compression_level": variant["compression_level"],
            "expected_metadata_codec": variant["expected_metadata_codec"],
            "write_kwargs": {
                **SHARED_WRITE_KWARGS,
                "compression": variant["compression"],
                "compression_level": variant["compression_level"],
            },
            "write_wall_s_informational": written["write_wall_s_informational"],
            "write_timing_note": written["write_timing_note"],
            "metadata": meta,
        }

    comparison = compare_pair_metadata(
        derived["parquet_none"]["metadata"],
        derived["parquet_zstd"]["metadata"],
    )
    if not comparison["unique_variable_ok"]:
        raise SystemExit(
            "两份新 Parquet 的行组/列编码不一致，或 codec 不符合预定配置，"
            "不能称为只有压缩配置不同。详情：{}".format(json.dumps(comparison, ensure_ascii=False))
        )

    original_ref = maybe_inspect_original_parquet(data_dir, derived["parquet_none"]["metadata"])

    # 源表只在父进程用于写出和记摘要；随后释放，避免与 worker 抢同一进程内存。
    del table
    gc.collect()

    manifest = {
        "script_version": SCRIPT_VERSION,
        "source": {
            "kind": source["kind"],
            "name": source["name"],
            "path": str(source["path"]),
            "bytes": source["bytes"],
            "sha256": source_sha,
            "num_rows": source_rows,
            "schema": source_schema,
            "digest": source_digest,
            "note": "统一源。只读，未改写。对照 Parquet 均由该表当场写出。",
        },
        "data_manifest_path": str(source["manifest_path"]) if source["manifest_path"] else None,
        "data_manifest_rows": None if not source["manifest"] else source["manifest"].get("rows"),
        "data_manifest_seed": None if not source["manifest"] else source["manifest"].get("seed"),
        "shared_write_kwargs": SHARED_WRITE_KWARGS,
        "zstd": zstd_info,
        "variants": {
            key: {
                **{k: v for k, v in item.items() if k != "path"},
                "path": str(item["path"]),
            }
            for key, item in derived.items()
        },
        "metadata_comparison": comparison,
        "original_data_parquet_reference": original_ref,
        "unique_variable_claim": (
            "本轮计时对照的两份文件由同一 pa.Table、同一路径函数、同一组 SHARED_WRITE_KWARGS 写出，"
            "差别仅是预定压缩配置：NONE（compression_level=None）与 ZSTD（compression_level=3）。"
            "原始 data/orders.parquet 不作计时基线。"
        ),
    }
    manifest_path = output_dir / DERIVED_MANIFEST_NAME
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "source": source,
        "source_digest": source_digest,
        "source_rows": source_rows,
        "source_sha256": source_sha,
        "derived": derived,
        "comparison": comparison,
        "original_reference": original_ref,
        "manifest_path": manifest_path,
        "manifest": manifest,
    }


def parent_main(args: argparse.Namespace) -> int:
    data_dir = args.data_dir.expanduser().resolve()
    output_dir = args.output_dir.expanduser().resolve()
    if args.runs < MIN_RUNS:
        raise SystemExit("--runs 至少为 {}，当前为 {}".format(MIN_RUNS, args.runs))
    if output_dir == data_dir:
        raise SystemExit("输出目录不能与 --data-dir 相同，以免污染或不小心覆盖原始数据。")
    try:
        output_dir.relative_to(data_dir)
        raise SystemExit(
            "输出目录 {} 位于数据目录 {} 之内，拒绝写入，以免污染原始数据。".format(
                output_dir, data_dir
            )
        )
    except ValueError:
        pass

    zstd_info = require_zstd(ZSTD_LEVEL)
    find_source(data_dir)

    output_dir.mkdir(parents=True, exist_ok=True)
    refuse_if_exists(output_dir)

    schedule = build_schedule(args.runs, args.seed)
    sample_interval_s = args.sample_interval_ms / 1000.0
    import psutil
    import pyarrow as pa

    configure_single_thread()
    print(
        "准备热缓存 Parquet 压缩对照：源目录={}；变体 {} × {} 次；输出目录：{}".format(
            data_dir, " / ".join(VARIANT_KEYS), args.runs, output_dir
        ),
        flush=True,
    )
    print(
        "ZSTD 可用：level {}（构建范围 {}–{}，默认 {}）。".format(
            zstd_info["requested_level"],
            zstd_info["minimum_level"],
            zstd_info["maximum_level"],
            zstd_info["default_level"],
        ),
        flush=True,
    )

    prepared = prepare_derived_pair(data_dir, output_dir, zstd_info)
    derived = prepared["derived"]
    source = prepared["source"]
    source_digest = prepared["source_digest"]

    trials = []
    tmp_dir = output_dir / ".benchmark-tmp"
    if tmp_dir.exists():
        import shutil

        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir()
    try:
        for item in schedule:
            key = item["variant"]
            info = derived[key]
            print(
                "  trial {trial}/{total}  {variant}  rep={rep}  先预热再测量 ...".format(
                    trial=item["trial"],
                    total=len(schedule),
                    variant=key,
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
            result_file = tmp_dir / "{}-rep{}.json".format(key, item["rep"])
            payload = run_worker(
                variant=key,
                path=info["path"],
                result_file=result_file,
                sample_interval_s=sample_interval_s,
            )
            trial = {
                "trial": item["trial"],
                "rep": item["rep"],
                "variant": key,
                "format": "parquet",
                "file_name": info["name"],
                "file_bytes": info["bytes"],
                "compression": info["compression"],
                "compression_level": info["compression_level"],
                "warmed_bytes": warmed,
                **payload,
            }
            if trial["num_rows"] != prepared["source_rows"]:
                raise RuntimeError(
                    "worker 读回行数与源表不符：{} vs {}".format(
                        trial["num_rows"], prepared["source_rows"]
                    )
                )
            if trial["digest"] != source_digest:
                raise RuntimeError(
                    "trial {}（{}）的逻辑摘要与源表不一致。".format(item["trial"], key)
                )
            trials.append(trial)
            print(
                "    {variant}  elapsed={elapsed:.4f}s  end_rss_delta={delta:.2f} MiB  "
                "table={table:.2f} MiB  samples={samples}".format(
                    variant=key,
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
                {"trial": trial["trial"], "variant": trial["variant"], "rep": trial["rep"]}
            )
        digests_match = len(digest_groups) == 1 and trials[0]["digest"] == source_digest

        validate_file = tmp_dir / "validation.json"
        print("  启动独立校验进程，逐批比较两个新 Parquet 与源表 ...", flush=True)
        strong_equals = run_strong_validation(
            source_path=source["path"],
            none_path=derived["parquet_none"]["path"],
            zstd_path=derived["parquet_zstd"]["path"],
            result_file=validate_file,
        )

        summary = summarize(trials)
        manifest = source["manifest"]
        report = {
            "script_version": SCRIPT_VERSION,
            "environment": {
                "python": sys.version,
                "python_executable": sys.executable,
                "platform": platform.platform(),
                "pyarrow_version": pa.__version__,
                "psutil_version": psutil.__version__,
                "os_cpu_count": os.cpu_count(),
                "pandas_installed": importlib.util.find_spec("pandas") is not None,
                "zstd": zstd_info,
            },
            "limits": {
                "script_version": SCRIPT_VERSION,
                "use_threads": False,
                "pyarrow_cpu_count": 1,
                "pyarrow_io_thread_count": 1,
                "parquet_memory_map": False,
                "parquet_pre_buffer": False,
                "sample_interval_ms": args.sample_interval_ms,
                "sampling_window": "read_only_before_digest",
                "sampling_protocol": "READY -> GO -> (timed read) -> READ_DONE -> SAMPLED -> digest -> DONE",
                "preload_known_lazy_modules": list(KNOWN_LAZY_MODULES),
                "preload_policy": (
                    "所有 worker 在 RSS 基线和计时前导入已定位的惰性模块，并触碰对应 FileFormat 对象；"
                    "不打开数据文件，不预分配 Table。只排除已知模块加载，不声称消除所有首次初始化。"
                ),
                "io_bytes_not_measured": True,
                "cpu_breakdown_not_measured": True,
                "note": "读取路径显式 use_threads=False、memory_map=False、pre_buffer=False。热缓存全列读取。",
            },
            "cache_definition": (
                "warm：每个 worker 启动前，由父进程将该目标文件按 8MiB 块顺序读到 EOF，"
                "依赖操作系统页缓存。预热时不做文件哈希。这不是磁盘冷读。"
            ),
            "hypothesis": (
                "ZSTD 文件通常更小，但热缓存全列读取需要解压，耗时可能更长、持平或更短；"
                "RSS 也不一定随文件变小而下降。没有 IO 请求量或 CPU 分解，不能事先承诺快慢，"
                "也不能把耗时差异全部归因于少读磁盘。以实测中位数为准，并保留最大值。"
            ),
            "data_dir": str(data_dir),
            "source": {
                "kind": source["kind"],
                "name": source["name"],
                "path": str(source["path"]),
                "bytes": source["bytes"],
                "sha256": prepared["source_sha256"],
                "num_rows": prepared["source_rows"],
                "digest": source_digest,
            },
            "manifest_path": str(source["manifest_path"]) if source["manifest_path"] else None,
            "manifest_rows": None if not manifest else manifest.get("rows"),
            "manifest_seed": None if not manifest else manifest.get("seed"),
            "derived": {
                "shared_write_kwargs": SHARED_WRITE_KWARGS,
                "metadata_comparison": prepared["comparison"],
                "original_data_parquet_reference": (
                    None
                    if not prepared["original_reference"]
                    else {
                        "path": prepared["original_reference"]["path"],
                        "bytes": prepared["original_reference"]["bytes"],
                        "used_as_timing_baseline": False,
                        "layout_matches_new_none": prepared["original_reference"]["layout_matches_new_none"],
                        "note": prepared["original_reference"]["note"],
                    }
                ),
                "unique_variable_claim": prepared["manifest"]["unique_variable_claim"],
            },
            "files": {
                key: {
                    "name": info["name"],
                    "path": str(info["path"]),
                    "bytes": info["bytes"],
                    "mib": bytes_mib(info["bytes"]),
                    "mib_definition": "1 MiB = 1024*1024 bytes",
                    "sha256": info["sha256"],
                    "compression": info["compression"],
                    "compression_level": info["compression_level"],
                    "expected_metadata_codec": info["expected_metadata_codec"],
                    "write_kwargs": info["write_kwargs"],
                    "num_row_groups": info["metadata"]["num_row_groups"],
                    "row_group_num_rows": info["metadata"]["row_group_num_rows"],
                    "unique_compressions": info["metadata"]["unique_compressions"],
                }
                for key, info in derived.items()
            },
            "seed": args.seed,
            "runs_per_variant": args.runs,
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
                "digests_match_source": digests_match,
                "digest_groups": len(digest_groups),
                "source_digest": source_digest,
                "digest": trials[0]["digest"] if digests_match else None,
                "digest_mismatch": None
                if digests_match
                else {"groups": [{"digest_key": key, "trials": refs} for key, refs in digest_groups.items()]},
                "strong_equals": strong_equals,
            },
            "metric_notes": {
                "elapsed_s": (
                    "time.perf_counter 仅覆盖读取并物化成全列 Table；"
                    "不含源表加载、Parquet 写入、import、已知惰性模块预加载、父进程预热、摘要和 equals 校验。"
                ),
                "rss_baseline_bytes": (
                    "worker 在导入 pyarrow/parquet 等、设置单线程，"
                    "并显式导入 pyarrow.dataset 等已定位惰性模块之后、正式读取之前的 RSS。"
                ),
                "rss_end_bytes": "读取结束后、仍持有 Table、开始摘要之前的 RSS。",
                "rss_end_delta_bytes": "rss_end_bytes - rss_baseline_bytes，最直接可解释的驻留增量。",
                "sampled_peak_rss_bytes": (
                    "父进程仅在读窗口（发出 GO 之后到收到 READ_DONE，此时 Table 仍被持有、摘要未开始）"
                    "轮询子进程 RSS 得到的最大值；间隔约 {} ms，可能漏掉短尖峰。"
                    "不是硬件级峰值，也不包含摘要或校验。"
                ).format(args.sample_interval_ms),
                "ru_maxrss_bytes": (
                    "Linux getrusage(RUSAGE_SELF).ru_maxrss（按 KB×1024 换算），"
                    "在读取结束、摘要开始之前记录。"
                    "这是截至该时刻的进程生命周期高水位，包含 import 与分配器保留，"
                    "不是把基线清零之后的读取峰值，也不等于 sampled_peak。"
                ),
                "ru_maxrss_after_digest_bytes": (
                    "摘要完成后再读一次 ru_maxrss，同样是生命周期高水位，可能高于读后值。"
                ),
                "table_nbytes": "pyarrow.Table.nbytes，是表内 buffer 的逻辑字节，不是整个进程 RSS。",
                "io_and_cpu": (
                    "未测量磁盘或操作系统读取字节，也未分解解压 CPU。"
                    "热缓存下不要把进程 read_bytes 直接当成 IO 请求量，"
                    "也不能把耗时差异全部归因于少读磁盘。"
                ),
            },
            "output": {
                "summary_json": str(output_dir / RESULT_JSON),
                "trials_csv": str(output_dir / RESULT_CSV),
                "derived_manifest": str(prepared["manifest_path"]),
            },
        }
        if not digests_match or not strong_equals.get("passed"):
            (output_dir / RESULT_JSON).write_text(
                json.dumps(report, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            write_trials_csv(output_dir / RESULT_CSV, trials)
            raise SystemExit(
                "内容校验失败。digests_match_source={}，strong_equals={}。结果已写入 {}。".format(
                    digests_match,
                    strong_equals.get("passed"),
                    output_dir / RESULT_JSON,
                )
            )
        refuse_if_exists(output_dir, result_only_paths(output_dir))
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
        description=(
            "compression-v1：从已有 orders.arrow 写出一对 Parquet（NONE 与 ZSTD level 3），"
            "在热缓存下全列读成 pyarrow.Table，比较耗时与内存。"
        )
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--validate", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--variant", choices=VARIANT_KEYS, help=argparse.SUPPRESS)
    parser.add_argument("--input", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--source", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--parquet-none", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--parquet-zstd", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--result-file", type=Path, help=argparse.SUPPRESS)
    parser.add_argument(
        "--data-dir",
        type=Path,
        help="已有数据目录，需包含 orders.arrow 作为统一源；该目录只读，不会被改写",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="结果与派生 Parquet 的目录，默认 results/warm-zstd-时间戳；已有文件时拒绝覆盖，可指定 ./results/warm-04-zstd",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=DEFAULT_RUNS,
        help="每种变体的测量次数，至少 {}，默认 {}".format(MIN_RUNS, DEFAULT_RUNS),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="打乱每轮变体顺序的随机种子，默认 {}".format(DEFAULT_SEED),
    )
    parser.add_argument(
        "--sample-interval-ms",
        type=float,
        default=DEFAULT_SAMPLE_INTERVAL_MS,
        help="父进程采样子进程 RSS 的间隔（毫秒），默认 {}".format(DEFAULT_SAMPLE_INTERVAL_MS),
    )
    args = parser.parse_args(argv)
    if args.worker:
        if not args.variant or not args.input or not args.result_file:
            parser.error("内部 --worker 需要 --variant、--input、--result-file")
        return args
    if args.validate:
        if not args.source or not args.parquet_none or not args.parquet_zstd or not args.result_file:
            parser.error("内部 --validate 需要 --source、--parquet-none、--parquet-zstd、--result-file")
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
