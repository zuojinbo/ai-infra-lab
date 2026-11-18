#!/usr/bin/env python3
"""生成同一份订单数据，并分别写成 CSV、Parquet、Arrow IPC 文件。

仅负责数据生成，不做读取性能基准测试。

依赖：Python 标准库 + pyarrow（不要额外安装 numpy）。

设计要点：
- 金额用整数分（amount_cents），避免浮点金额。
- event_time 用 Unix 秒（int64），避免 CSV 往返时被推断成 timestamp。
- Parquet 显式 compression="NONE"、row_group_size=100000；
  但仍可能使用字典等编码，因此这不是纯行式/列式布局对照。
- 分批生成，减少一次性堆积大量 Python 对象。
- 目标文件已存在时拒绝覆盖，以免毁掉已有实验数据。
"""

from __future__ import annotations

import argparse
import array
import hashlib
import json
import platform
import random
import shutil
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

DEFAULT_ROWS = 1_000_000
DEFAULT_SEED = 42
DEFAULT_OUTPUT_DIR = "data"
BATCH_SIZE = 100_000
ROW_GROUP_SIZE = 100_000

# event_time 起点：2024-01-01 00:00:00 UTC
BASE_EVENT_TIME = 1_704_067_200
EVENT_WINDOW_SECONDS = 30 * 24 * 3600

CITIES = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "南京"]
CATEGORIES = ["电子", "服装", "食品", "图书", "家居", "运动"]

SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.int64()),
        pa.field("user_id", pa.int64()),
        pa.field("amount_cents", pa.int64()),
        pa.field("city", pa.string()),
        pa.field("event_time", pa.int64()),
        pa.field("category", pa.string()),
    ]
)

FILES = {
    "csv": "orders.csv",
    "parquet": "orders.parquet",
    "arrow_ipc": "orders.arrow",
}
MANIFEST_NAME = "manifest.json"

CSV_READ_CONVERT_OPTIONS = pacsv.ConvertOptions(
    column_types={field.name: field.type for field in SCHEMA}
)


def generate_batch(start_id: int, n: int, rng: random.Random) -> pa.Table:
    """按列生成一批数据，再立刻转成 Arrow Table。"""
    order_id = pa.array(range(start_id, start_id + n), type=pa.int64())
    # array.array('q') 是 int64，避免为每个整数创建一个 Python int 对象。
    user_id = pa.array(
        array.array("q", (rng.randint(1, 100_000) for _ in range(n))),
        type=pa.int64(),
    )
    amount_cents = pa.array(
        array.array("q", (rng.randint(100, 100_000) for _ in range(n))),
        type=pa.int64(),
    )
    event_time = pa.array(
        array.array(
            "q",
            (BASE_EVENT_TIME + rng.randint(0, EVENT_WINDOW_SECONDS) for _ in range(n)),
        ),
        type=pa.int64(),
    )
    city = pa.array((rng.choice(CITIES) for _ in range(n)), type=pa.string())
    category = pa.array((rng.choice(CATEGORIES) for _ in range(n)), type=pa.string())
    return pa.table(
        {
            "order_id": order_id,
            "user_id": user_id,
            "amount_cents": amount_cents,
            "city": city,
            "event_time": event_time,
            "category": category,
        },
        schema=SCHEMA,
    )


def generate_table(n_rows: int, seed: int, batch_size: int = BATCH_SIZE) -> pa.Table:
    rng = random.Random(seed)
    batches = []
    written = 0
    next_id = 1
    while written < n_rows:
        size = min(batch_size, n_rows - written)
        print(f"  生成行 {written + 1}–{written + size} ...", flush=True)
        batches.append(generate_batch(next_id, size, rng))
        next_id += size
        written += size
    table = pa.concat_tables(batches)
    if table.schema != SCHEMA:
        table = table.cast(SCHEMA)
    if table.num_rows != n_rows:
        raise RuntimeError(f"生成行数不符：期望 {n_rows}，实际 {table.num_rows}")
    return table


def target_paths(output_dir: Path) -> list[Path]:
    names = list(FILES.values()) + [MANIFEST_NAME]
    return [output_dir / name for name in names]


def refuse_if_exists(output_dir: Path) -> None:
    existing = [path for path in target_paths(output_dir) if path.exists()]
    if not existing:
        return
    lines = [
        "目标文件已存在，拒绝覆盖，以免破坏已有实验数据：",
        *[f"  - {path}" for path in existing],
        "如需重新生成，请先把这些文件移走或更换 --output-dir。",
    ]
    raise SystemExit("\n".join(lines))


def write_csv(table: pa.Table, path: Path) -> None:
    # CSV 本身无块压缩；写出纯文本、带表头。
    pacsv.write_csv(table, path)


def write_parquet(table: pa.Table, path: Path) -> None:
    # 默认 compression 是 snappy，必须显式关闭。
    # use_dictionary 保持 PyArrow 默认 True，编码情况写入 manifest。
    pq.write_table(
        table,
        path,
        compression="NONE",
        row_group_size=ROW_GROUP_SIZE,
        use_dictionary=True,
    )


def write_arrow_ipc(table: pa.Table, path: Path) -> None:
    options = pa.ipc.IpcWriteOptions(compression=None)
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema, options=options) as writer:
            writer.write_table(table)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_parquet(path: Path) -> dict:
    parquet_file = pq.ParquetFile(path)
    metadata = parquet_file.metadata
    row_groups = []
    for i in range(metadata.num_row_groups):
        group = metadata.row_group(i)
        columns = []
        for j in range(group.num_columns):
            column = group.column(j)
            columns.append(
                {
                    "name": column.path_in_schema,
                    "compression": column.compression,
                    "encodings": list(column.encodings),
                    "total_compressed_size": column.total_compressed_size,
                    "total_uncompressed_size": column.total_uncompressed_size,
                }
            )
        row_groups.append({"num_rows": group.num_rows, "columns": columns})
    return {
        "num_rows": metadata.num_rows,
        "num_row_groups": metadata.num_row_groups,
        "row_group_num_rows": [item["num_rows"] for item in row_groups],
        "row_groups": row_groups,
    }


def inspect_arrow_ipc(path: Path) -> dict:
    with pa.memory_map(str(path), "r") as source:
        reader = pa.ipc.open_file(source)
        return {
            "schema": [f"{field.name}: {field.type}" for field in reader.schema],
            "num_record_batches": reader.num_record_batches,
            "num_rows": reader.read_all().num_rows,
        }


def build_manifest(
    *,
    output_dir: Path,
    seed: int,
    n_rows: int,
    batch_size: int,
    written: dict[str, Path],
) -> dict:
    files = {}
    for label, path in written.items():
        files[path.name] = {
            "label": label,
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
    return {
        "environment": {
            "python": sys.version,
            "python_executable": sys.executable,
            "platform": platform.platform(),
            "pyarrow_version": pa.__version__,
        },
        "seed": seed,
        "rows": n_rows,
        "batch_size": batch_size,
        "schema": [{"name": field.name, "type": str(field.type)} for field in SCHEMA],
        "schema_notes": {
            "amount_cents": "整数分，不是浮点金额。",
            "event_time": "Unix 秒（int64），不是 timestamp 类型，保证 CSV 按显式 schema 回读时类型一致。",
            "row_order": "未按 event_time 预排序，时间值在 30 天窗口内随机分布。",
        },
        "writer_config": {
            "csv": {
                "filename": FILES["csv"],
                "compression": None,
                "include_header": True,
            },
            "parquet": {
                "filename": FILES["parquet"],
                "compression": "NONE",
                "row_group_size": ROW_GROUP_SIZE,
                "use_dictionary": True,
                "note": (
                    "已关闭压缩，但 PyArrow 默认仍可能对列使用字典/RLE 等编码；"
                    "因此本轮比较的是三套格式及其默认编码，不是纯行式/列式布局对照。"
                ),
            },
            "arrow_ipc": {
                "filename": FILES["arrow_ipc"],
                "format": "Arrow IPC file (random access)",
                "compression": None,
            },
        },
        "files": files,
        "parquet_actual": inspect_parquet(written["parquet"]),
        "arrow_ipc_actual": inspect_arrow_ipc(written["arrow_ipc"]),
        "csv_read_hint": {
            "note": "回读 CSV 时必须传入与生成时相同的显式 schema，否则类型可能被推断错。",
            "convert_options_column_types": {
                field.name: str(field.type) for field in SCHEMA
            },
        },
    }


def write_all(table: pa.Table, output_dir: Path, seed: int, n_rows: int) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    refuse_if_exists(output_dir)

    staging = output_dir / ".generate-tmp"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir()

    try:
        staged = {
            "csv": staging / FILES["csv"],
            "parquet": staging / FILES["parquet"],
            "arrow_ipc": staging / FILES["arrow_ipc"],
        }
        print(f"写入 {staged['csv'].name} ...", flush=True)
        write_csv(table, staged["csv"])
        print(f"写入 {staged['parquet'].name} ...", flush=True)
        write_parquet(table, staged["parquet"])
        print(f"写入 {staged['arrow_ipc'].name} ...", flush=True)
        write_arrow_ipc(table, staged["arrow_ipc"])

        manifest = build_manifest(
            output_dir=output_dir,
            seed=seed,
            n_rows=n_rows,
            batch_size=BATCH_SIZE,
            written=staged,
        )
        manifest_path = staging / MANIFEST_NAME
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

        # 再次检查正式目录，避免生成过程中出现同名文件后被覆盖。
        refuse_if_exists(output_dir)
        for path in list(staged.values()) + [manifest_path]:
            path.replace(output_dir / path.name)
    except Exception:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)
        raise
    finally:
        if staging.exists():
            shutil.rmtree(staging, ignore_errors=True)

    return output_dir / MANIFEST_NAME


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="生成同一份订单数据，并写出 CSV / Parquet / Arrow IPC 文件。"
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_ROWS,
        help=f"行数，默认 {DEFAULT_ROWS}",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(DEFAULT_OUTPUT_DIR),
        help=f"输出目录，默认 {DEFAULT_OUTPUT_DIR}",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"随机种子，默认 {DEFAULT_SEED}",
    )
    args = parser.parse_args(argv)
    if args.rows <= 0:
        parser.error("--rows 必须是正整数")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    output_dir = args.output_dir.expanduser().resolve()
    print(
        f"准备生成 {args.rows} 行订单数据；seed={args.seed}；输出目录：{output_dir}",
        flush=True,
    )
    refuse_if_exists(output_dir)
    table = generate_table(args.rows, args.seed)
    manifest_path = write_all(table, output_dir, args.seed, args.rows)

    print("完成。写出文件：")
    for path in target_paths(output_dir):
        print(f"  {path}  ({path.stat().st_size} bytes)")
    print(f"清单：{manifest_path}")
    print(
        "注意：刚写完的文件很可能已经进入操作系统页缓存，"
        "随后立刻读取是热读，不能当作磁盘冷读。"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
