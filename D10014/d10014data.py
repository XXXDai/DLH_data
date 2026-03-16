from datetime import datetime, timedelta, timezone
from pathlib import Path
import csv
import gzip

DATA_DIR = Path("data/src/bybit_spot_trade_di")  # 数据目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
DATE = "2026-02-10"  # 数据日期，日期


def build_file_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    file_name = f"{symbol}_{date_str}.csv.gz"
    return base_dir / symbol / file_name


def iter_trades(file_path: Path, size_key: str):
    with gzip.open(file_path, "rt", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("交易文件缺少表头")
        if "timestamp" not in reader.fieldnames or "price" not in reader.fieldnames:
            raise ValueError("交易文件缺少必要字段: timestamp/price")
        if size_key not in reader.fieldnames:
            raise ValueError(f"交易文件缺少必要字段: {size_key}")
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


def format_datetime(ts: float | None) -> str:
    if ts is None:
        return "无"
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S+00:00")


def parse_ts_seconds(ts_text: str) -> float:
    ts_value = float(ts_text)
    return ts_value / 1000 if ts_value > 1e12 else ts_value


def aggregate_trades(file_path: Path) -> dict:
    size_key = detect_size_key(file_path)
    total = 0
    size_sum = 0.0
    foreign_sum = 0.0
    min_ts = None
    max_ts = None
    side_stats = {}
    for row in iter_trades(file_path, size_key):
        total += 1
        ts = parse_ts_seconds(row["timestamp"])
        size = float(row[size_key])
        foreign = float(row["foreignNotional"])
        min_ts = ts if min_ts is None else min(min_ts, ts)
        max_ts = ts if max_ts is None else max(max_ts, ts)
        size_sum += size
        foreign_sum += foreign
        side = row.get("side", "")
        if side:
            stats = side_stats.setdefault(side, {"size": 0.0, "foreignNotional": 0.0})
            stats["size"] += size
            stats["foreignNotional"] += foreign
    return {
        "total": total,
        "size_sum": size_sum,
        "foreign_sum": foreign_sum,
        "min_ts": min_ts,
        "max_ts": max_ts,
        "side_stats": side_stats,
    }


def read_date_range(symbol: str, start_date: str, end_date: str) -> dict:
    """
    读取指定日期范围的成交数据
    Args:
        symbol: 交易对，如 'BTCUSDT'
        start_date: 开始日期，如 '2025-01-01'
        end_date: 结束日期，如 '2025-01-05'
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    total_stats = {
        "total": 0,
        "size_sum": 0.0,
        "foreign_sum": 0.0,
        "min_ts": None,
        "max_ts": None,
        "side_stats": {},
    }
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        file_path = build_file_path(DATA_DIR, symbol, date_str)
        if file_path.exists():
            stats = aggregate_trades(file_path)
            print(f"已读取: {date_str}")
            total_stats["total"] += stats["total"]
            total_stats["size_sum"] += stats["size_sum"]
            total_stats["foreign_sum"] += stats["foreign_sum"]
            if stats["min_ts"] is not None:
                total_stats["min_ts"] = (
                    stats["min_ts"]
                    if total_stats["min_ts"] is None
                    else min(total_stats["min_ts"], stats["min_ts"])
                )
            if stats["max_ts"] is not None:
                total_stats["max_ts"] = (
                    stats["max_ts"]
                    if total_stats["max_ts"] is None
                    else max(total_stats["max_ts"], stats["max_ts"])
                )
            for side, values in stats["side_stats"].items():
                merged = total_stats["side_stats"].setdefault(
                    side, {"size": 0.0, "foreignNotional": 0.0}
                )
                merged["size"] += values["size"]
                merged["foreignNotional"] += values["foreignNotional"]
        else:
            print(f"跳过 {date_str}: 文件不存在")
        current += timedelta(days=1)
    return total_stats


def main() -> None:
    file_path = build_file_path(DATA_DIR, SYMBOL, DATE)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return
    stats = aggregate_trades(file_path)
    print(f"成交笔数: {stats['total']}")
    print(f"成交量: {stats['size_sum']:.4f}")
    print(f"成交额: {stats['foreign_sum']:.2f}")
    print(f"时间范围: {format_datetime(stats['min_ts'])} ~ {format_datetime(stats['max_ts'])}")
    print("买卖方向统计:")
    for side, values in stats["side_stats"].items():
        print(f"{side} size={values['size']:.4f} foreignNotional={values['foreignNotional']:.2f}")


if __name__ == "__main__":
    main()
