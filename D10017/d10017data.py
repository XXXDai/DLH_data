from datetime import datetime
from pathlib import Path
import csv

DATA_DIR = Path("data/src/bybit_future_fundingrate_di")  # 数据目录，路径
SYMBOL = "BTCUSDT"  # 交易对，字符串
START_DATE = "2026-01-15"  # 开始日期，日期
END_DATE = "2026-01-20"  # 结束日期，日期


def build_file_path(base_dir: Path, symbol: str) -> Path:
    file_name = f"{symbol}_fundingrate.csv"
    return base_dir / symbol / file_name


def read_rows(file_path: Path) -> list:
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def main() -> None:
    file_path = build_file_path(DATA_DIR, SYMBOL)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return
    rows = read_rows(file_path)
    rows = [row for row in rows if START_DATE <= row.get("date", "") <= END_DATE]
    daily_sum = {}
    daily_cnt = {}
    for row in rows:
        date_str = row.get("date", "")
        rate = float(row.get("fundingRate", "0") or 0)
        daily_sum[date_str] = daily_sum.get(date_str, 0.0) + rate
        daily_cnt[date_str] = daily_cnt.get(date_str, 0) + 1
    print(f"日期范围: {START_DATE} ~ {END_DATE}")
    for date_str in sorted(daily_sum.keys()):
        avg = daily_sum[date_str] / daily_cnt[date_str]
        print(f"{date_str} 日均资金费率: {avg:.6f}")
    annualized = []
    for row in rows:
        rate = float(row.get("fundingRate", "0") or 0)
        annualized.append(rate * 3 * 365 * 100)
    if annualized:
        mean_rate = sum(annualized) / len(annualized)
        print(f"区间年化资金费率均值(%): {mean_rate:.2f}")


if __name__ == "__main__":
    main()
