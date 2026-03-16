from pathlib import Path
import json
import pandas as pd

DATA_DIR = Path("data/src/polymarket_orderbook_rt")  # 数据目录，路径
HOUR_TAG = "2026012110"  # 目标小时，小时
BATCH_ID = 1  # 批次编号，编号


def build_file_path(hour_tag: str, batch_id: int) -> Path:
    file_name = f"polymarket_orderbook_rt-{hour_tag}-batch_{batch_id:04d}.json"
    return DATA_DIR / hour_tag / file_name


def read_single_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return pd.DataFrame()
    return pd.read_json(file_path, lines=True)


def parse_best_prices(row: pd.Series) -> pd.Series:
    data = row.get("data")
    if isinstance(data, str):
        data = json.loads(data)
    if not isinstance(data, dict):
        return pd.Series({"best_bid": None, "best_ask": None})
    bids = data.get("bids", [])
    asks = data.get("asks", [])
    best_bid = float(bids[0]["price"]) if bids else None
    best_ask = float(asks[0]["price"]) if asks else None
    return pd.Series({"best_bid": best_bid, "best_ask": best_ask})


def main() -> None:
    file_path = build_file_path(HOUR_TAG, BATCH_ID)
    df = read_single_file(file_path)
    if df.empty:
        return
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    print(df["type"].value_counts())
    prices = df.apply(parse_best_prices, axis=1)
    df = pd.concat([df, prices], axis=1)
    print(df[["symbol", "type", "best_bid", "best_ask"]].head(10))


if __name__ == "__main__":
    main()
