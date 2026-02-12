from datetime import datetime, timedelta, timezone
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc

DATA_DIR = Path("data/dws/dws_polymarket_orderbook_ss_1s_hi")  # 数据目录，路径
HOUR_TAG = "2026010815"  # 目标小时，小时
RANGE_START = "2026-01-08 15:00"  # 区间开始，日期时间
RANGE_END = "2026-01-08 15:00"  # 区间结束，日期时间
SYMBOL_FILTER = ""  # 市场过滤，字符串
TOP_N = 10  # Top数量，个数


def build_file_path(hour_tag: str) -> Path:
    return DATA_DIR / hour_tag / f"polymarket_orderbook_ss_1s_{hour_tag}.parquet"


def read_single_hour(hour_tag: str) -> pd.DataFrame:
    file_path = build_file_path(hour_tag)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return pd.DataFrame()
    return pd.read_parquet(file_path)


def read_hour_range(start_hour: str, end_hour: str) -> pd.DataFrame:
    start = datetime.strptime(start_hour, "%Y-%m-%d %H:%M")
    end = datetime.strptime(end_hour, "%Y-%m-%d %H:%M")
    dfs = []
    current = start
    while current <= end:
        hour_tag = current.strftime("%Y%m%d%H")
        file_path = build_file_path(hour_tag)
        if file_path.exists():
            dfs.append(pd.read_parquet(file_path))
            print(f"已读取: {hour_tag}")
        else:
            print(f"跳过 {hour_tag}: 文件不存在")
        current += timedelta(hours=1)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def read_dataset_filtered(start_ts: int, end_ts: int) -> pd.DataFrame:
    if not DATA_DIR.exists():
        print(f"目录不存在: {DATA_DIR}")
        return pd.DataFrame()
    dataset = ds.dataset(DATA_DIR, format="parquet")
    table = dataset.to_table(
        columns=["ts", "symbol", "best_bid", "best_ask"],
        filter=(pc.field("ts") >= start_ts) & (pc.field("ts") < end_ts),
    )
    return table.to_pandas()


def high_freq_analysis(df: pd.DataFrame) -> None:
    if df.empty:
        print("数据为空")
        return
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df["spread"] = df["best_ask"] - df["best_bid"]
    print(f"平均价差: {df['spread'].mean():.4f}")
    print(f"市场数量: {df['symbol'].nunique()}")
    df["mid_price"] = (df["best_bid"] + df["best_ask"]) / 2
    df["second"] = df["datetime"].dt.floor("1s")
    df["price_change"] = df["mid_price"].diff()
    df["return"] = df["mid_price"].pct_change()
    df["volatility_10s"] = df["return"].rolling(10).std()
    df["volatility_60s"] = df["return"].rolling(60).std()
    std = df["return"].std()
    jumps = df[df["return"].abs() > 3 * std] if std and std > 0 else df.iloc[0:0]
    print(f"跳跃次数: {len(jumps)}")
    spread_by_market = df.groupby("symbol").agg(
        best_bid=("best_bid", "mean"),
        best_ask=("best_ask", "mean"),
        spread_mean=("spread", "mean"),
    )
    print(spread_by_market.sort_values("spread_mean", ascending=False).head(TOP_N))


def main() -> None:
    df = read_single_hour(HOUR_TAG)
    if not df.empty:
        high_freq_analysis(df)
    df_range = read_hour_range(RANGE_START, RANGE_END)
    if SYMBOL_FILTER:
        df_range = df_range[df_range["symbol"] == SYMBOL_FILTER]
    if not df_range.empty:
        high_freq_analysis(df_range)
    start_ts = int(datetime(2026, 1, 8, 15, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime(2026, 1, 8, 15, 1, tzinfo=timezone.utc).timestamp() * 1000)
    df_filtered = read_dataset_filtered(start_ts, end_ts)
    if not df_filtered.empty:
        print(f"过滤记录数: {len(df_filtered)}")


if __name__ == "__main__":
    main()
