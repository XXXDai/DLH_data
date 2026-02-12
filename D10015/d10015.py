from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import gzip
import importlib
import time
import pyarrow as pa
import pyarrow.parquet as pq
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

INPUT_DIR = Path("data/src/bybit_future_trade_di")  # 输入目录，路径
OUTPUT_DIR = Path("data/dws/dws_bybit_future_trade_1s_di")  # 输出目录，路径
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
START_DATE = "2026-02-11"  # 起始日期（含），日期
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def seconds_until_next_utc_midnight() -> int:
    now = datetime.now(tz=timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = int((next_midnight - now).total_seconds())
    return seconds if seconds > 0 else 1


def build_input_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    file_name = f"{symbol}{date_str}.csv.gz"
    return base_dir / symbol / file_name


def build_output_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    date_tag = date_str.replace("-", "")
    file_name = f"{date_tag}_{symbol}_trade_1s.parquet"
    return base_dir / symbol / date_tag / file_name


def iter_dates(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def iter_trades(file_path: Path):
    with gzip.open(file_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def build_schema() -> pa.Schema:
    return pa.schema(
        [
            ("ts", pa.int64()),
            ("symbol", pa.string()),
            ("price", pa.float64()),
            ("size", pa.float64()),
        ]
    )


def write_parquet(records: list, writer: pq.ParquetWriter, schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(records, schema=schema)
    writer.write_table(table)


def process_date(symbol: str, date_str: str) -> None:
    input_path = build_input_path(INPUT_DIR, symbol, date_str)
    if not input_path.exists():
        return
    if input_path.stat().st_size == 0:
        log(f"文件为空: {input_path}")
        input_path.unlink()
        return
    output_path = build_output_path(OUTPUT_DIR, symbol, date_str)
    if output_path.exists():
        return
    tmp_output_path = output_path.with_name(output_path.name + ".part")
    if tmp_output_path.exists():
        tmp_output_path.unlink()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = build_schema()
    writer = pq.ParquetWriter(tmp_output_path, schema, compression="snappy")
    last_ts = None
    sum_size = 0.0
    sum_value = 0.0
    total = 0
    for row in iter_trades(input_path):
        ts = int(float(row["timestamp"]))
        price = float(row["price"])
        size = float(row["size"])
        value = price * size
        if last_ts is None:
            last_ts = ts
        if ts != last_ts:
            avg_price = sum_value / sum_size if sum_size > 0 else 0.0
            write_parquet(
                [
                    {
                        "ts": last_ts,
                        "symbol": symbol,
                        "price": avg_price,
                        "size": sum_value,
                    }
                ],
                writer,
                schema,
            )
            total += 1
            last_ts = ts
            sum_size = 0.0
            sum_value = 0.0
        sum_size += size
        sum_value += value
    if last_ts is not None:
        avg_price = sum_value / sum_size if sum_size > 0 else 0.0
        write_parquet(
            [
                {
                    "ts": last_ts,
                    "symbol": symbol,
                    "price": avg_price,
                    "size": sum_value,
                }
            ],
            writer,
            schema,
        )
        total += 1
    writer.close()
    tmp_output_path.replace(output_path)
    log(f"已写入: {output_path}，记录数: {total}")


def parse_date_from_name(name: str) -> str | None:
    if not name.endswith(".csv.gz"):
        return None
    date_str = name.removesuffix(".csv.gz")
    if len(date_str) < 10:
        return None
    date_str = date_str[-10:]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    return date_str


def iter_available_dates(symbol: str) -> list:
    symbol_dir = INPUT_DIR / symbol
    if not symbol_dir.exists():
        return []
    dates = set()
    for path in symbol_dir.glob(f"{symbol}*.csv.gz"):
        date_str = parse_date_from_name(path.name)
        if date_str:
            dates.add(date_str)
    dates_list = sorted(dates)
    return [d for d in dates_list if d >= START_DATE]


def main() -> None:
    if not SYMBOLS:
        log("未配置交易对")
        return
    while True:
        for symbol in SYMBOLS:
            for date_str in iter_available_dates(symbol):
                process_date(symbol, date_str)
        sleep_seconds = seconds_until_next_utc_midnight()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 00:00）")
        time.sleep(sleep_seconds)

def run() -> None:
    main()


if __name__ == "__main__":
    run()
