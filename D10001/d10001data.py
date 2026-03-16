from datetime import datetime, timezone
from pathlib import Path
import zipfile
import orjson

DATA_DIR = Path("data/src/bybit_future_orderbook_di")  # 数据目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
DATE = "2026-02-11"  # 数据日期，日期
DEPTH = 200  # 订单簿深度，档位


def build_file_path(base_dir: Path, symbol: str, date: str, depth: int) -> Path:
    file_name = f"{date}_{symbol}_ob{depth}.data.zip"
    return base_dir / symbol / file_name


def iter_messages(file_path: Path):
    with zipfile.ZipFile(file_path, "r") as zf:
        for name in zf.namelist():
            with zf.open(name) as f:
                for line in f:
                    yield orjson.loads(line)


def read_orderbook_file(file_path: Path) -> list:
    """
    读取订单簿数据文件（本地）
    返回解析后的消息列表
    """
    return list(iter_messages(file_path))


def update_orderbook(orderbook: dict, msg: dict) -> None:
    """
    按快照+增量消息更新订单簿
    """
    msg_type = msg.get("type")
    data = msg.get("data", {})
    if msg_type == "snapshot":
        orderbook["bids"] = {float(b[0]): float(b[1]) for b in data.get("b", [])}
        orderbook["asks"] = {float(a[0]): float(a[1]) for a in data.get("a", [])}
    elif msg_type == "delta":
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


def format_datetime(ts: int | None) -> str:
    if ts is None:
        return "无"
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")


def main() -> None:
    file_path = build_file_path(DATA_DIR, SYMBOL, DATE, DEPTH)
    total = 0
    snapshot_count = 0
    delta_count = 0
    min_ts = None
    max_ts = None
    spread_sum = 0.0
    spread_count = 0
    orderbook = {"bids": {}, "asks": {}}
    for msg in iter_messages(file_path):
        total += 1
        ts = msg.get("ts")
        if ts is not None:
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)
        msg_type = msg.get("type")
        if msg_type == "snapshot":
            snapshot_count += 1
        elif msg_type == "delta":
            delta_count += 1
        data = msg.get("data", {})
        bids = data.get("b", [])
        asks = data.get("a", [])
        if bids and asks:
            spread_sum += float(asks[0][0]) - float(bids[0][0])
            spread_count += 1
        update_orderbook(orderbook, msg)
    mean_spread = spread_sum / spread_count if spread_count else float("nan")
    print(f"消息总数: {total}")
    print(f"快照数: {snapshot_count}")
    print(f"增量更新数: {delta_count}")
    print(f"时间范围: {format_datetime(min_ts)} ~ {format_datetime(max_ts)}")
    print(f"平均价差: {mean_spread:.4f}")
    print(f"买单档位数: {len(orderbook['bids'])}")
    print(f"卖单档位数: {len(orderbook['asks'])}")
    best_bid = max(orderbook["bids"].keys()) if orderbook["bids"] else float("nan")
    best_ask = min(orderbook["asks"].keys()) if orderbook["asks"] else float("nan")
    print(f"最优买价: {best_bid:.2f}")
    print(f"最优卖价: {best_ask:.2f}")


if __name__ == "__main__":
    main()
