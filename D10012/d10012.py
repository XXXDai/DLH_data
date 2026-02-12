from datetime import datetime, timedelta
from pathlib import Path
import zipfile
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from sortedcontainers import SortedDict

INPUT_DIR = Path("data/src/bybit_spot_orderbook_di")  # 输入目录，路径
OUTPUT_DIR = Path("data/dwd/dwd_bybit_spot_ob_ss_di")  # 输出目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
DEPTH = 200  # 订单簿深度，档位
START_DATE = "2026-02-11"  # 起始日期（含），日期
BATCH_SIZE = 20000  # 批次大小，条


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
        "symbol": data.get("s", msg.get("symbol", SYMBOL)),
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


def process_date(date_str: str) -> None:
    input_path = build_input_path(INPUT_DIR, SYMBOL, date_str)
    if not input_path.exists():
        print(f"文件不存在: {input_path}")
        return
    output_path = build_output_path(OUTPUT_DIR, SYMBOL, date_str)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    schema = build_schema()
    writer = pq.ParquetWriter(output_path, schema, compression="snappy")
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
    print(f"已写入: {output_path}，记录数: {total}")


def main() -> None:
    end_date = datetime.now().strftime("%Y-%m-%d")
    for date_str in iter_dates(START_DATE, end_date):
        print(f"开始处理: {date_str}")
        process_date(date_str)


if __name__ == "__main__":
    main()
