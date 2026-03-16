from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/src/bybit_future_orderbook_rt")  # 数据目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
HOUR = "2026021203"  # 小时分区，小时


def build_file_path(base_dir: Path, symbol: str, hour_str: str) -> Path:
    file_name = f"{symbol}-bybit_future_orderbook_rt-{hour_str}.json"
    return base_dir / symbol / hour_str / file_name


def parse_best_prices(row: pd.Series) -> pd.Series:
    data = row["data"]
    bids = data.get("b", [])
    asks = data.get("a", [])
    return pd.Series(
        {
            "best_bid": float(bids[0][0]) if bids else None,
            "best_ask": float(asks[0][0]) if asks else None,
        }
    )


def main() -> None:
    file_path = build_file_path(DATA_DIR, SYMBOL, HOUR)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return
    df = pd.read_json(file_path, lines=True)
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    print(df["type"].value_counts())
    prices = df.apply(parse_best_prices, axis=1)
    df = pd.concat([df, prices], axis=1)


if __name__ == "__main__":
    main()
