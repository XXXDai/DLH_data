from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/src/polymarket_orderbook_rt_ss")  # 数据目录，路径
HOUR_TAG = "2026012110"  # 目标小时，小时
BATCH_ID = 1  # 批次编号，编号
TOP_N = 10  # Top数量，个数


def build_file_path(hour_tag: str, batch_id: int) -> Path:
    file_name = f"polymarket_orderbook_rt_ss-{hour_tag}-batch_{batch_id:04d}.json"
    return DATA_DIR / hour_tag / file_name


def read_single_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return pd.DataFrame()
    return pd.read_json(file_path, lines=True)


def calc_imbalance(row: pd.Series) -> float:
    bids = row.get("bids") or []
    asks = row.get("asks") or []
    bids = bids[:5] if bids else []
    asks = asks[:5] if asks else []
    bid_vol = sum(float(b[1]) for b in bids)
    ask_vol = sum(float(a[1]) for a in asks)
    total = bid_vol + ask_vol
    return (bid_vol - ask_vol) / total if total > 0 else 0.0


def main() -> None:
    file_path = build_file_path(HOUR_TAG, BATCH_ID)
    df = read_single_file(file_path)
    if df.empty:
        return
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["spread"] = df["best_ask"] - df["best_bid"]
    market_stats = (
        df.groupby("symbol")
        .agg(ts=("ts", "count"), spread=("spread", "mean"), bid_depth=("bid_depth", "mean"))
        .reset_index()
    )
    market_stats.columns = ["symbol", "updates", "avg_spread", "avg_depth"]
    print(market_stats.nlargest(TOP_N, "updates"))
    df["imbalance"] = df.apply(calc_imbalance, axis=1)
    print(df[["symbol", "spread", "imbalance"]].head(10))


if __name__ == "__main__":
    main()
