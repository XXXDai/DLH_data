from io import TextIOWrapper
from pathlib import Path
import csv
import gzip
import re
import time
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from cex import cex_config
from cex.cex_common import build_part_path
from cex.cex_common import cleanup_stale_part_file
from cex.cex_common import download_file_from_storage
from cex.cex_common import is_valid_gzip_file
from cex.cex_common import list_storage_file_names
from cex.cex_common import replace_output_file
from cex.cex_common import seconds_until_next_utc_4h
from cex.cex_common import storage_file_exists
from cex.cex_trade_common import normalize_trade_for_agg


INPUT_DATASET_ID = "D10014"  # 输入数据集标识，字符串
OUTPUT_DATASET_ID = "D10016"  # 输出数据集标识，字符串
BITGET_ARCHIVE_NAME_PATTERN = re.compile(r"^(\d{8})_\d{3}\.zip$")  # Bitget归档文件名格式，正则
DATE_TEXT_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 日期文本格式，正则
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    """输出日志消息。"""
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def list_input_symbols(base_dir: Path) -> list:
    """列出输入目录内的交易对。"""
    if not base_dir.exists():
        return []
    return sorted([path.name for path in base_dir.iterdir() if path.is_dir() and not path.name.startswith("__")])


def resolve_symbols(exchange: str, base_dir: Path) -> list:
    """解析单个交易所的交易对列表。"""
    return sorted(set(cex_config.get_spot_symbols(exchange)) | set(list_input_symbols(base_dir)))


def build_input_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造输入文件路径。"""
    return base_dir / symbol / f"{symbol}_{date_str}.csv.gz"


def build_bitget_input_paths(base_dir: Path, symbol: str, date_str: str) -> list[Path]:
    """列出Bitget原始分片路径。"""
    csv_path = build_input_path(base_dir, symbol, date_str)
    if storage_file_exists(csv_path):
        return [csv_path]
    paths = []
    for file_name in list_storage_file_names(base_dir / symbol):
        if not file_name.endswith(".zip"):
            continue
        if parse_bitget_date_from_name(file_name) == date_str:
            paths.append(base_dir / symbol / file_name)
    return sorted(paths)


def build_output_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造输出文件路径。"""
    date_tag = date_str.replace("-", "")
    return base_dir / symbol / date_tag / f"{date_tag}_{symbol}_trade_1s.parquet"


def iter_gzip_trades(file_path: Path):
    """遍历GZip成交记录。"""
    with gzip.open(file_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def iter_zip_trades(file_path: Path):
    """遍历Zip成交记录。"""
    with zipfile.ZipFile(file_path, "r") as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8", newline=""))
            for row in reader:
                yield row


def iter_trades(exchange: str, file_path: Path):
    """按交易所遍历成交记录。"""
    if file_path.name.endswith(".zip"):
        yield from iter_zip_trades(file_path)
        return
    yield from iter_gzip_trades(file_path)


def list_input_paths(exchange: str, base_dir: Path, symbol: str, date_str: str) -> list[Path]:
    """列出单日输入文件列表。"""
    if exchange == "bitget":
        input_path = build_input_path(base_dir, symbol, date_str)
        if input_path.exists():
            return [input_path]
        return build_bitget_input_paths(base_dir, symbol, date_str)
    input_path = build_input_path(base_dir, symbol, date_str)
    return [input_path] if input_path.exists() else []


def build_schema() -> pa.Schema:
    """构造Parquet表结构。"""
    return pa.schema(
        [
            ("ts", pa.int64()),
            ("symbol", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
        ]
    )


def write_parquet(records: list, writer: pq.ParquetWriter, schema: pa.Schema) -> None:
    """写入Parquet批次。"""
    writer.write_table(pa.Table.from_pylist(records, schema=schema))


def process_date(exchange: str, symbol: str, date_str: str) -> None:
    """处理单日成交数据。"""
    input_dir = cex_config.get_source_dir(INPUT_DATASET_ID, exchange)
    output_dir = cex_config.get_output_dir(OUTPUT_DATASET_ID, exchange)
    if not input_dir or not output_dir:
        return
    input_paths = list_input_paths(exchange, input_dir, symbol, date_str)
    if not input_paths:
        return
    for input_path in input_paths:
        if not input_path.exists() and not download_file_from_storage(input_path):
            return
    for input_path in input_paths:
        if input_path.stat().st_size == 0:
            log(f"文件为空: {input_path}")
            input_path.unlink()
            return
        if input_path.name.endswith(".csv.gz") and not is_valid_gzip_file(input_path):
            log(f"压缩文件损坏，已删除待重下: {input_path}")
            input_path.unlink()
            return
    output_path = build_output_path(output_dir, symbol, date_str)
    cleanup_stale_part_file(output_path)
    if storage_file_exists(output_path):
        return
    tmp_output_path = build_part_path(output_path)
    if tmp_output_path.exists():
        tmp_output_path.unlink()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = build_schema()
    writer = pq.ParquetWriter(tmp_output_path, schema, compression="snappy")
    last_ts = None
    sum_size = 0.0
    sum_value = 0.0
    total = 0
    for input_path in input_paths:
        for row in iter_trades(exchange, input_path):
            ts_ms, price, size = normalize_trade_for_agg(exchange, row)
            ts = int(ts_ms / 1000)
            value = price * size
            if last_ts is None:
                last_ts = ts
            if ts != last_ts:
                avg_price = sum_value / sum_size if sum_size > 0 else 0.0
                write_parquet([{"ts": last_ts, "symbol": symbol, "price": avg_price, "size": sum_value}], writer, schema)
                total += 1
                last_ts = ts
                sum_size = 0.0
                sum_value = 0.0
            sum_size += size
            sum_value += value
    if last_ts is not None:
        avg_price = sum_value / sum_size if sum_size > 0 else 0.0
        write_parquet([{"ts": last_ts, "symbol": symbol, "price": avg_price, "size": sum_value}], writer, schema)
        total += 1
    writer.close()
    replace_output_file(tmp_output_path, output_path)
    log(f"{exchange} 已写入: {output_path}，记录数: {total}")


def parse_bitget_date_from_name(name: str) -> str | None:
    """从Bitget文件名解析日期。"""
    matched = BITGET_ARCHIVE_NAME_PATTERN.match(name)
    if not matched:
        return None
    raw_date = matched.group(1)
    return f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"


def parse_date_from_name(exchange: str, name: str) -> str | None:
    """从文件名解析日期。"""
    if exchange == "bitget":
        if name.endswith(".csv.gz"):
            date_str = name.removesuffix(".csv.gz")[-10:]
            if DATE_TEXT_PATTERN.fullmatch(date_str):
                return date_str
        return parse_bitget_date_from_name(name)
    if not name.endswith(".csv.gz"):
        return None
    date_str = name.removesuffix(".csv.gz")[-10:]
    if not DATE_TEXT_PATTERN.fullmatch(date_str):
        return None
    return date_str


def iter_available_dates(exchange: str, base_dir: Path, symbol: str) -> list:
    """遍历可处理的日期列表。"""
    dates = set()
    for file_name in list_storage_file_names(base_dir / symbol):
        date_str = parse_date_from_name(exchange, file_name)
        if date_str:
            dates.add(date_str)
    return sorted(dates)


def run_exchange(exchange: str) -> None:
    """执行单个交易所聚合任务。"""
    if not cex_config.is_supported(OUTPUT_DATASET_ID, exchange):
        return
    input_dir = cex_config.get_source_dir(INPUT_DATASET_ID, exchange)
    if not input_dir:
        return
    symbols = resolve_symbols(exchange, input_dir)
    for symbol in symbols:
        for date_str in iter_available_dates(exchange, input_dir, symbol):
            process_date(exchange, symbol, date_str)


def main() -> None:
    """运行成交聚合主循环。"""
    while True:
        for exchange in cex_config.list_exchanges():
            run_exchange(exchange)
        sleep_seconds = seconds_until_next_utc_4h()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 4小时倍数）")
        time.sleep(sleep_seconds)


def run() -> None:
    """兼容启动器运行入口。"""
    main()


if __name__ == "__main__":
    run()
