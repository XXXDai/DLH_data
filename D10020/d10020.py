from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import orjson
import pyarrow as pa
import pyarrow.parquet as pq

INPUT_DIR = Path("data/src/polymarket_orderbook_rt_ss")  # 输入目录，路径
OUTPUT_DIR = Path("data/dws/dws_polymarket_orderbook_ss_1m_hi")  # 输出目录，路径
START_HOUR = "2026-02-12 00:00"  # 起始小时，日期时间
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def seconds_until_next_utc_hour() -> int:
    now = datetime.now(tz=timezone.utc)
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    seconds = int((next_hour - now).total_seconds())
    return seconds if seconds > 0 else 1


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
                bucket = (ts_ms // 60000) * 60000
                key = (symbol, bucket)
                current = bucket_map.get(key)
                if current is None or ts_ms >= current[0]:
                    bucket_map[key] = (ts_ms, record)
    output_records = []
    for _, record in bucket_map.values():
        output_records.append(normalize_record(record))
    output_records.sort(key=lambda r: (r["ts"], r["symbol"]))
    output_path = OUTPUT_DIR / hour_str / f"polymarket_orderbook_ss_1m_{hour_str}.parquet"
    if output_path.exists():
        return
    tmp_output_path = output_path.with_name(output_path.name + ".part")
    if tmp_output_path.exists():
        tmp_output_path.unlink()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(output_records, schema=build_schema())
    pq.write_table(table, tmp_output_path, compression="snappy")
    tmp_output_path.replace(output_path)
    log(f"已写入: {output_path}，记录数: {len(output_records)}")


def parse_start_hour_tag() -> str:
    return datetime.strptime(START_HOUR, "%Y-%m-%d %H:%M").strftime("%Y%m%d%H")


def last_complete_hour_tag() -> str:
    now_hour = datetime.now(tz=timezone.utc).replace(minute=0, second=0, microsecond=0)
    return (now_hour - timedelta(hours=1)).strftime("%Y%m%d%H")


def list_available_hours() -> list:
    if not INPUT_DIR.exists():
        return []
    hours = []
    for path in INPUT_DIR.iterdir():
        name = path.name
        if not path.is_dir():
            continue
        if len(name) != 10 or not name.isdigit():
            continue
        hours.append(name)
    hours.sort()
    return hours


def main() -> None:
    start_tag = parse_start_hour_tag()
    while True:
        end_tag = last_complete_hour_tag()
        for hour_str in list_available_hours():
            if hour_str < start_tag:
                continue
            if hour_str > end_tag:
                continue
            process_hour(hour_str)
        sleep_seconds = seconds_until_next_utc_hour()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 整点）")
        time.sleep(sleep_seconds)

def run() -> None:
    main()


if __name__ == "__main__":
    run()
