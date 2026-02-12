from datetime import datetime, timedelta, timezone
from pathlib import Path
import time
import zipfile
import importlib
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from sortedcontainers import SortedDict
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

INPUT_DIR = Path("data/src/bybit_future_orderbook_di")  # 输入目录，路径
OUTPUT_DIR = Path("data/dwd/dwd_bybit_future_ob_ss_di")  # 输出目录，路径
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
DEPTH = 200  # 订单簿深度，档位
START_DATE = "2026-02-11"  # 起始日期（含），日期
BATCH_SIZE = 20000  # 批次大小，条
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
    file_name = f"{date_str}_{symbol}_ob{DEPTH}.data.zip"
    return base_dir / symbol / file_name


def build_output_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    date_tag = date_str.replace("-", "")
    file_name = f"{date_tag}_{symbol}_ob{DEPTH}_snapshot.parquet"
    return base_dir / symbol / date_tag / file_name


def iter_dates(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def iter_messages(file_path: Path):
    with zipfile.ZipFile(file_path, "r") as zf:
        for name in zf.namelist():
            with zf.open(name) as f:
                for line in f:
                    yield orjson.loads(line)


def update_orderbook(orderbook: dict, msg_type: str, data: dict) -> bool:
    if msg_type == "snapshot":
        orderbook["bids"] = SortedDict({float(b[0]): float(b[1]) for b in data.get("b", [])})
        orderbook["asks"] = SortedDict({float(a[0]): float(a[1]) for a in data.get("a", [])})
        return True
    if msg_type == "delta":
        for price, size in data.get("b", []):
            price, size = float(price), float(size)
            if size == 0:
                orderbook["bids"].pop(price, None)
            else:
                orderbook["bids"][price] = size
        for price, size in data.get("a", []):
            price, size = float(price), float(size)
            if size == 0:
                orderbook["asks"].pop(price, None)
            else:
                orderbook["asks"][price] = size
        return True
    return False


def build_snapshot(orderbook: dict, msg: dict, data: dict) -> dict:
    bids = orderbook["bids"]
    asks = orderbook["asks"]
    best_bid = bids.peekitem(-1)[0] if bids else None
    best_ask = asks.peekitem(0)[0] if asks else None
    bid_list = [{"price": price, "qty": qty} for price, qty in reversed(bids.items())]
    ask_list = [{"price": price, "qty": qty} for price, qty in asks.items()]
    return {
        "symbol": data.get("s", msg.get("symbol")),
        "update_type": msg.get("type"),
        "ts": msg.get("ts"),
        "cts": msg.get("cts"),
        "update_id": data.get("u"),
        "seq": data.get("seq"),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": len(bids),
        "ask_depth": len(asks),
        "bids": bid_list,
        "asks": ask_list,
    }


def build_schema() -> pa.Schema:
    side_type = pa.list_(pa.struct([("price", pa.float64()), ("qty", pa.float64())]))
    return pa.schema(
        [
            ("symbol", pa.string()),
            ("update_type", pa.string()),
            ("ts", pa.int64()),
            ("cts", pa.int64()),
            ("update_id", pa.int64()),
            ("seq", pa.int64()),
            ("best_bid", pa.float64()),
            ("best_ask", pa.float64()),
            ("bid_depth", pa.int32()),
            ("ask_depth", pa.int32()),
            ("bids", side_type),
            ("asks", side_type),
        ]
    )


def write_parquet(records: list, writer: pq.ParquetWriter, schema: pa.Schema) -> None:
    table = pa.Table.from_pylist(records, schema=schema)
    writer.write_table(table)

def process_date(symbol: str, date_str: str) -> None:
    input_path = build_input_path(INPUT_DIR, symbol, date_str)
    if not input_path.exists():
        log(f"文件不存在: {input_path}")
        return
    if not zipfile.is_zipfile(input_path):
        log(f"文件不是有效zip: {input_path}")
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
    orderbook = {"bids": SortedDict(), "asks": SortedDict()}
    has_snapshot = False
    batch = []
    total = 0
    for msg in iter_messages(input_path):
        msg_type = msg.get("type")
        data = msg.get("data", {})
        if msg_type == "snapshot":
            has_snapshot = update_orderbook(orderbook, msg_type, data)
        elif msg_type == "delta" and has_snapshot:
            update_orderbook(orderbook, msg_type, data)
        else:
            continue
        batch.append(build_snapshot(orderbook, msg, data))
        total += 1
        if len(batch) >= BATCH_SIZE:
            write_parquet(batch, writer, schema)
            batch.clear()
    if batch:
        write_parquet(batch, writer, schema)
    writer.close()
    tmp_output_path.replace(output_path)
    log(f"已写入: {output_path}，记录数: {total}")


def parse_date_from_name(name: str) -> str | None:
    parts = name.split("_", 1)
    if not parts:
        return None
    date_str = parts[0]
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
    for path in symbol_dir.glob(f"*_ob{DEPTH}.data.zip"):
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
