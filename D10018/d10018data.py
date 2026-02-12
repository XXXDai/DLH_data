from pathlib import Path
import csv

DATA_DIR = Path("data/src/bybit_insurance_di")  # 数据目录，路径
COIN = "USDT"  # 币种，字符串
TARGET_DATE = "2026-01-20"  # 目标日期，日期


def build_file_path(base_dir: Path, coin: str) -> Path:
    file_name = f"{coin}_insurance.csv"
    return base_dir / coin / file_name


def read_rows(file_path: Path) -> list:
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def main() -> None:
    file_path = build_file_path(DATA_DIR, COIN)
    if not file_path.exists():
        print(f"文件不存在: {file_path}")
        return
    rows = read_rows(file_path)
    day_rows = [row for row in rows if row.get("collect_date") == TARGET_DATE]
    print(f"目标日期记录数: {len(day_rows)}")
    perpetual_rows = [row for row in rows if row.get("contract_type") == "perpetual"]
    futures_rows = [row for row in rows if row.get("contract_type") == "futures"]
    print(f"永续合约记录数: {len(perpetual_rows)}")
    print(f"交割合约记录数: {len(futures_rows)}")
    daily_total = {}
    for row in rows:
        date_str = row.get("collect_date", "")
        value = float(row.get("pool_value", "0") or 0)
        daily_total[date_str] = daily_total.get(date_str, 0.0) + value
    if daily_total:
        latest_date = max(daily_total.keys())
        print(f"最新日期总保险基金价值: {daily_total[latest_date]:.2f}")
    latest_by_symbol = {}
    for row in rows:
        symbol = row.get("symbol", "")
        value = float(row.get("pool_value", "0") or 0)
        latest_by_symbol[symbol] = value
    top_pools = sorted(latest_by_symbol.items(), key=lambda x: x[1], reverse=True)[:10]
    print("保险基金最高的交易对:")
    for symbol, value in top_pools:
        print(f"{symbol} {value:.2f}")


if __name__ == "__main__":
    main()
