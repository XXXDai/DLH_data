from datetime import datetime, timedelta, timezone
from pathlib import Path
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_DIR = Path("data/src/polymarket_orderbook_rt_ss")  # 输入目录，路径
OUTPUT_DIR = Path("data/dws/dws_polymarket_orderbook_ss_1s_hi")  # 输出目录，路径
START_HOUR = "2026-02-12 00:00"  # 起始小时，日期时间
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def iter_hours(start_hour: str, end_hour: str):
    start = datetime.strptime(start_hour, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_hour, "%Y-%m-%d %H:%M")
    current = start
    while current <= end:
        yield current.strftime("%Y%m%d%H")
        current += timedelta(hours=1)


def list_input_files(hour_str: str) -> list:
    hour_dir = INPUT_DIR / hour_str
    if not hour_dir.exists():
        log(f"目录不存在: {hour_dir}")
        return []
    return sorted(hour_dir.glob(f"polymarket_orderbook_rt_ss-{hour_str}-batch_*.json"))


def to_float_pairs(levels) -> list:
    if not levels:
        return []
    pairs = []
    for item in levels:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            price, size = item[0], item[1]
        elif isinstance(item, dict):
            price, size = item.get("price"), item.get("size")
        else:
            continue
        if price is None or size is None:
            continue
        pairs.append([float(price), float(size)])
    return pairs


def get_ts_ms(record: dict) -> int | None:
    ts_ms = record.get("ts")
    if ts_ms is None:
        ts_ms = record.get("collect_ts")
    if ts_ms is None:
        return None
    return int(ts_ms)


def build_schema() -> pa.Schema:
    side_type = pa.list_(pa.list_(pa.float64()))
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("update_type", pa.string()),
            ("ts", pa.int64()),
            ("cts", pa.int64()),
            ("collect_ts", pa.int64()),
            ("update_id", pa.int64()),
            ("seq", pa.int64()),
            ("best_bid", pa.float64()),
            ("best_ask", pa.float64()),
            ("bid_depth", pa.int64()),
            ("ask_depth", pa.int64()),
            ("bids", side_type),
            ("asks", side_type),
        ]
    )


def normalize_record(record: dict) -> dict:
    bids = to_float_pairs(record.get("bids"))
    asks = to_float_pairs(record.get("asks"))
    bid_depth = record.get("bid_depth")
    ask_depth = record.get("ask_depth")
    if bid_depth is None:
        bid_depth = len(bids)
    if ask_depth is None:
        ask_depth = len(asks)
    best_bid = record.get("best_bid")
    best_ask = record.get("best_ask")
    return {
        "symbol": record.get("symbol"),
        "update_type": record.get("update_type"),
        "ts": int(record.get("ts")) if record.get("ts") is not None else None,
        "cts": int(record.get("cts")) if record.get("cts") is not None else None,
        "collect_ts": int(record.get("collect_ts")) if record.get("collect_ts") is not None else None,
        "update_id": int(record.get("update_id")) if record.get("update_id") is not None else 0,
        "seq": int(record.get("seq")) if record.get("seq") is not None else 0,
        "best_bid": float(best_bid) if best_bid is not None else None,
        "best_ask": float(best_ask) if best_ask is not None else None,
        "bid_depth": int(bid_depth),
        "ask_depth": int(ask_depth),
        "bids": bids,
        "asks": asks,
    }


def process_hour(hour_str: str) -> None:
    input_files = list_input_files(hour_str)
    if not input_files:
        return
    bucket_map = {}
    for file_path in input_files:
        with file_path.open("rb") as f:
            for line in f:
                record = orjson.loads(line)
                if not isinstance(record, dict):
                    continue
                ts_ms = get_ts_ms(record)
                if ts_ms is None:
                    continue
                symbol = record.get("symbol")
                if not symbol:
                    continue
                bucket = (ts_ms // 1000) * 1000
                key = (symbol, bucket)
                current = bucket_map.get(key)
                if current is None or ts_ms >= current[0]:
                    bucket_map[key] = (ts_ms, record)
    output_records = []
    for _, record in bucket_map.values():
        output_records.append(normalize_record(record))
    output_records.sort(key=lambda r: (r["ts"], r["symbol"]))
    output_path = OUTPUT_DIR / hour_str / f"polymarket_orderbook_ss_1s_{hour_str}.parquet"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(output_records, schema=build_schema())
    pq.write_table(table, output_path, compression="snappy")
    log(f"已写入: {output_path}，记录数: {len(output_records)}")


def main() -> None:
    end_hour = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    end_hour_str = end_hour.strftime("%Y-%m-%d %H:%M")
    for hour_str in iter_hours(START_HOUR, end_hour_str):
        log(f"开始处理: {hour_str}")
        process_hour(hour_str)

def run() -> None:
    main()


if __name__ == "__main__":
    run()
