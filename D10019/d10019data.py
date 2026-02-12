from pathlib import Path
import csv

DATA_DIR = Path("data/src/bybit_onchainstaking_di")  # 数据目录，路径
COIN = "ETH"  # 质押币种，字符串
TARGET_DATE = "2026-01-20"  # 目标日期，日期


def build_file_path(base_dir: Path, coin: str) -> Path:
    file_name = f"{coin}_onchainstaking.csv"
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
    for row in rows:
        apr_text = row.get("apr", "")
        row["apr_pct"] = float(apr_text or 0) * 100
    latest_date = max((row.get("collect_date", "") for row in rows), default="")
    latest_rows = [row for row in rows if row.get("collect_date") == latest_date]
    if latest_rows:
        latest_apr = latest_rows[0]["apr_pct"]
        print(f"{COIN} 最新 APR: {latest_apr:.2f}%")
    apr_sum = {}
    apr_cnt = {}
    for row in rows:
        date_str = row.get("collect_date", "")
        apr_sum[date_str] = apr_sum.get(date_str, 0.0) + row.get("apr_pct", 0.0)
        apr_cnt[date_str] = apr_cnt.get(date_str, 0) + 1
    print("APR 日均趋势:")
    for date_str in sorted(apr_sum.keys()):
        avg = apr_sum[date_str] / apr_cnt[date_str]
        print(f"{date_str} {avg:.2f}%")


if __name__ == "__main__":
    main()
