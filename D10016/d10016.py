from datetime import datetime, timedelta
from pathlib import Path
import csv
import gzip
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_DIR = Path("data/src/bybit_spot_trade_di")  # 输入目录，路径
OUTPUT_DIR = Path("data/dws/dws_bybit_spot_trade_1s_di")  # 输出目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
START_DATE = "2026-02-11"  # 起始日期（含），日期


def build_input_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    file_name = f"{symbol}_{date_str}.csv.gz"
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


def detect_size_key(file_path: Path) -> str:
    with gzip.open(file_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
    if "size" in fieldnames:
        return "size"
    if "volume" in fieldnames:
        return "volume"
    raise ValueError("交易文件缺少 size 或 volume 字段")


def parse_ts_seconds(ts_text: str) -> int:
    ts_value = float(ts_text)
    return int(ts_value / 1000) if ts_value > 1e12 else int(ts_value)


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


def process_date(date_str: str) -> None:
    input_path = build_input_path(INPUT_DIR, SYMBOL, date_str)
    if not input_path.exists():
        print(f"文件不存在: {input_path}")
        return
    output_path = build_output_path(OUTPUT_DIR, SYMBOL, date_str)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = build_schema()
    writer = pq.ParquetWriter(output_path, schema, compression="snappy")
    size_key = detect_size_key(input_path)
    last_ts = None
    sum_size = 0.0
    sum_value = 0.0
    total = 0
    for row in iter_trades(input_path):
        ts = parse_ts_seconds(row["timestamp"])
        price = float(row["price"])
        size = float(row[size_key])
        value = price * size
        if last_ts is None:
            last_ts = ts
        if ts != last_ts:
            avg_price = sum_value / sum_size if sum_size > 0 else 0.0
            write_parquet(
                [
                    {
                        "ts": last_ts,
                        "symbol": SYMBOL,
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
                    "symbol": SYMBOL,
                    "price": avg_price,
                    "size": sum_value,
                }
            ],
            writer,
            schema,
        )
        total += 1
    writer.close()
    print(f"已写入: {output_path}，记录数: {total}")


def main() -> None:
    end_date = datetime.now().strftime("%Y-%m-%d")
    for date_str in iter_dates(START_DATE, end_date):
        print(f"开始处理: {date_str}")
        process_date(date_str)


if __name__ == "__main__":
    main()
