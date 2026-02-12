from datetime import datetime, timezone
from pathlib import Path
import csv
import json
import threading
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import app_config

BASE_URL = "https://api.bybit.com"  # API根地址，字符串
ENDPOINT = "/v5/market/funding/history"  # 接口路径，字符串
CATEGORY = "linear"  # 合约类型，字符串
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
START_DATE = app_config.D10017_START_DATE  # 起始日期（含），日期
LIMIT = 200  # 每页数量，条
TIMEOUT_SECONDS = 10  # 请求超时，秒
DATA_DIR = Path("data/src/bybit_future_fundingrate_di")  # 保存目录，路径
QUIET = False  # 静默模式开关，开关


def log(message: str) -> None:
    if not QUIET:
        print(message)


def build_file_path(base_dir: Path, symbol: str) -> Path:
    file_name = f"{symbol}_fundingrate.csv"
    return base_dir / symbol / file_name


def utc_datetime_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def utc_date_str(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


def load_existing_timestamps(file_path: Path) -> set:
    if not file_path.exists():
        return set()
    timestamps = set()
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_text = row.get("fundingRateTimestamp")
            if ts_text:
                timestamps.add(int(ts_text))
    return timestamps


def request_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("接口请求失败: 网络错误") from exc


def fetch_funding_history(symbol: str, start_ms: int, end_ms: int) -> list:
    records = []
    current_end = end_ms
    while True:
        query = urlencode(
            {
                "category": CATEGORY,
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": current_end,
                "limit": LIMIT,
            }
        )
        url = f"{BASE_URL}{ENDPOINT}?{query}"
        payload = request_json(url)
        if payload.get("retCode") != 0:
            raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
        result = payload.get("result", {})
        items = result.get("list", [])
        if not items:
            break
        for item in items:
            ts_ms = int(item["fundingRateTimestamp"])
            records.append(item)
        min_ts = min(int(item["fundingRateTimestamp"]) for item in items)
        if min_ts <= start_ms:
            break
        current_end = min_ts - 1
    return records


def write_records(file_path: Path, symbol: str, records: list, existing_ts: set) -> int:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not file_path.exists()
    rows = []
    for item in records:
        ts_ms = int(item["fundingRateTimestamp"])
        if ts_ms in existing_ts:
            continue
        rows.append(
            {
                "symbol": item.get("symbol", symbol),
                "fundingRateTimestamp": str(ts_ms),
                "datetime": utc_datetime_str(ts_ms),
                "date": utc_date_str(ts_ms),
                "fundingRate": item.get("fundingRate", ""),
            }
        )
    rows.sort(key=lambda x: int(x["fundingRateTimestamp"]))
    with file_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "fundingRateTimestamp",
                "datetime",
                "date",
                "fundingRate",
            ],
        )
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def run_symbol(symbol: str) -> None:
    file_path = build_file_path(DATA_DIR, symbol)
    existing_ts = load_existing_timestamps(file_path)
    if existing_ts:
        start_ms = max(existing_ts) + 1
    else:
        start_ms = int(datetime.strptime(START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if start_ms >= end_ms:
        log("没有新增数据")
        return
    records = fetch_funding_history(symbol, start_ms, end_ms)
    count = write_records(file_path, symbol, records, existing_ts)
    log(f"已写入记录数: {count}")


def main() -> None:
    if not SYMBOLS:
        log("未配置交易对")
        return
    threads = []
    for symbol in SYMBOLS:
        thread = threading.Thread(target=run_symbol, args=(symbol,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


if __name__ == "__main__":
    run()
