from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/src/bybit_future_orderbook_rt_ss")  # 数据目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
HOUR = "2026012015"  # 小时分区，小时
START_HOUR = "2026-01-20 15:00"  # 开始时间，时间
END_HOUR = "2026-01-20 18:00"  # 结束时间，时间


def build_file_path(base_dir: Path, symbol: str, hour_str: str) -> Path:
    file_name = f"{symbol}-bybit_future_orderbook_rt_ss-{hour_str}.json"
    return base_dir / symbol / hour_str / file_name


def read_hour_range(symbol: str, start_hour: str, end_hour: str) -> pd.DataFrame:
    """
    读取指定小时范围的订单簿快照
    Args:
        symbol: 交易对，如 'BTCUSDT'
        start_hour: 开始时间，如 '2026-01-20 15:00'
        end_hour: 结束时间，如 '2026-01-20 18:00'
    """
    start = datetime.strptime(start_hour, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_hour, "%Y-%m-%d %H:%M")
    dfs = []
    current = start
    while current <= end:
        hour_str = current.strftime("%Y%m%d%H")
        file_path = build_file_path(DATA_DIR, symbol, hour_str)
        if file_path.exists():
            dfs.append(pd.read_json(file_path, lines=True))
            print(f"已读取: {hour_str}")
        else:
            print(f"跳过 {hour_str}: 文件不存在")
        current += timedelta(hours=1)
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
        df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df
    return pd.DataFrame()


def main() -> None:
    file_path = build_file_path(DATA_DIR, SYMBOL, HOUR)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return
    df = pd.read_json(file_path, lines=True)
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    print(f"记录数: {len(df)}")
    print(f"时间范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
    df["spread"] = df["best_ask"] - df["best_bid"]
    df["spread_bps"] = df["spread"] / df["best_bid"] * 10000
    print(f"平均价差(bps): {df['spread_bps'].mean():.2f}")
    df["mid_price"] = (df["best_bid"] + df["best_ask"]) / 2


if __name__ == "__main__":
    main()
