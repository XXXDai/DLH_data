from datetime import datetime, timezone
from io import BytesIO, TextIOWrapper
from pathlib import Path
import csv
import json
import sys
import threading
import zipfile
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen

import app_config
from cex import cex_config
from cex.cex_common import download_file_from_storage
from cex.cex_common import download_bytes
from cex.cex_common import upload_file_to_s3
from cex.cex_common import seconds_until_next_utc_midnight


DATASET_ID = "D10017"  # 数据集标识，字符串
TIMEOUT_SECONDS = app_config.HTTP_TIMEOUT_SECONDS  # 请求超时，秒
BINANCE_BUCKET_URL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"  # Binance公共数据根地址，字符串
BINANCE_API_URL = "https://fapi.binance.com/fapi/v1/fundingRate"  # Binance资金费率接口地址，字符串
BITGET_API_URL = "https://api.bitget.com/api/v2/mix/market/history-fund-rate"  # Bitget资金费率接口地址，字符串
BYBIT_API_URL = "https://api.bybit.com/v5/market/funding/history"  # Bybit资金费率接口地址，字符串
OKX_API_URL = "https://www.okx.com/api/v5/public/funding-rate-history"  # OKX资金费率接口地址，字符串
BYBIT_CATEGORY = "linear"  # Bybit合约类型，字符串
BYBIT_LIMIT = 200  # Bybit分页大小，条
BITGET_PAGE_SIZE = 100  # Bitget分页大小，条
OKX_LIMIT = 100  # OKX分页大小，条
HTTP_HEADER_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"  # 请求头浏览器标识，字符串
QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    """输出日志消息。"""
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def status_update(exchange: str, market: str, symbol: str, value) -> None:
    """更新任务状态。"""
    if STATUS_HOOK:
        STATUS_HOOK(cex_config.get_status_key(exchange, market, symbol), value)


def build_file_path(base_dir: Path, symbol: str) -> Path:
    """构造资金费率文件路径。"""
    return base_dir / symbol / f"{symbol}_fundingrate.csv"


def utc_datetime_str(ts_ms: int) -> str:
    """格式化UTC时间字符串。"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def utc_date_str(ts_ms: int) -> str:
    """格式化UTC日期字符串。"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def latest_date_from_timestamps(timestamps: set[int]) -> str:
    """从时间戳集合中提取最新日期。"""
    if not timestamps:
        return ""
    return utc_date_str(max(timestamps))


def load_existing_timestamps(file_path: Path) -> set:
    """加载已有时间戳集合。"""
    if not file_path.exists():
        download_file_from_storage(file_path)
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


def request_json(url: str) -> dict | list:
    """请求JSON响应。"""
    request = Request(url, headers={"User-Agent": HTTP_HEADER_USER_AGENT})
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_bybit_rows(symbol: str, start_ms: int, end_ms: int) -> list:
    """抓取Bybit资金费率数据。"""
    rows = []
    current_end = end_ms
    while True:
        query = urlencode(
            {
                "category": BYBIT_CATEGORY,
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": current_end,
                "limit": BYBIT_LIMIT,
            }
        )
        payload = request_json(f"{BYBIT_API_URL}?{query}")
        if payload.get("retCode") != 0:
            ret_msg = str(payload.get("retMsg") or "")
            if "symbol invalid" in ret_msg.lower():
                return []
            raise RuntimeError(f"接口返回错误: {ret_msg}")
        items = payload.get("result", {}).get("list", [])
        if not items:
            break
        for item in items:
            rows.append(
                {
                    "symbol": item.get("symbol", symbol),
                    "fundingRateTimestamp": int(item["fundingRateTimestamp"]),
                    "fundingRate": item.get("fundingRate", ""),
                }
            )
        min_ts = min(int(item["fundingRateTimestamp"]) for item in items)
        if min_ts <= start_ms:
            break
        current_end = min_ts - 1
    return rows


def fetch_binance_bucket_rows(symbol: str, start_date: str) -> list:
    """抓取Binance月度资金费率文件。"""
    rows = []
    start_month = datetime.strptime(start_date, "%Y-%m-%d").strftime("%Y-%m")
    current_month = datetime.now(tz=timezone.utc).strftime("%Y-%m")
    year = int(start_month[0:4])
    month = int(start_month[5:7])
    end_year = int(current_month[0:4])
    end_month = int(current_month[5:7])
    while (year, month) <= (end_year, end_month):
        month_tag = f"{year:04d}-{month:02d}"
        url = (
            f"{BINANCE_BUCKET_URL}/data/futures/um/monthly/fundingRate/{symbol}/"
            f"{symbol}-fundingRate-{month_tag}.zip"
        )
        try:
            content = download_bytes(url, TIMEOUT_SECONDS)
        except RuntimeError as exc:
            if "HTTP 404" not in str(exc):
                raise
            if (year, month) == (end_year, end_month):
                break
            if month == 12:
                year += 1
                month = 1
            else:
                month += 1
            continue
        with zipfile.ZipFile(cex_zip_bytes(content), "r") as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    rows.append(
                        {
                            "symbol": symbol,
                            "fundingRateTimestamp": int(row["calc_time"]),
                            "fundingRate": row.get("last_funding_rate", ""),
                        }
                    )
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return rows


def fetch_binance_api_rows(symbol: str, start_ms: int, end_ms: int) -> list:
    """抓取Binance接口资金费率数据。"""
    rows = []
    current_start = start_ms
    while current_start < end_ms:
        query = urlencode({"symbol": symbol, "startTime": current_start, "endTime": end_ms, "limit": 1000})
        payload = request_json(f"{BINANCE_API_URL}?{query}")
        if not isinstance(payload, list) or not payload:
            break
        for item in payload:
            rows.append(
                {
                    "symbol": item.get("symbol", symbol),
                    "fundingRateTimestamp": int(item["fundingTime"]),
                    "fundingRate": item.get("fundingRate", ""),
                }
            )
        max_ts = max(int(item["fundingTime"]) for item in payload)
        if max_ts <= current_start:
            break
        current_start = max_ts + 1
    return rows


def fetch_bitget_rows(symbol: str, start_ms: int) -> list:
    """抓取Bitget资金费率数据。"""
    rows = []
    page_no = 1
    while True:
        query = urlencode(
            {
                "symbol": symbol,
                "productType": "USDT-FUTURES",
                "pageSize": BITGET_PAGE_SIZE,
                "pageNo": page_no,
            }
        )
        payload = request_json(f"{BITGET_API_URL}?{query}")
        if payload.get("code") != "00000":
            raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
        items = payload.get("data", [])
        if not items:
            break
        reached_start = False
        for item in items:
            ts_ms = int(item["fundingTime"])
            if ts_ms < start_ms:
                reached_start = True
                continue
            rows.append(
                {
                    "symbol": item.get("symbol", symbol),
                    "fundingRateTimestamp": ts_ms,
                    "fundingRate": item.get("fundingRate", ""),
                }
            )
        if reached_start or len(items) < BITGET_PAGE_SIZE:
            break
        page_no += 1
    return rows


def fetch_okx_rows(symbol: str, start_ms: int) -> list:
    """抓取OKX资金费率数据。"""
    rows = []
    after = None
    while True:
        params = {"instId": symbol, "limit": str(OKX_LIMIT)}
        if after:
            params["after"] = after
        payload = request_json(f"{OKX_API_URL}?{urlencode(params)}")
        if payload.get("code") != "0":
            raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
        items = payload.get("data", [])
        if not items:
            break
        reached_start = False
        for item in items:
            ts_ms = int(item["fundingTime"])
            if ts_ms < start_ms:
                reached_start = True
                continue
            rows.append(
                {
                    "symbol": item.get("instId", symbol),
                    "fundingRateTimestamp": ts_ms,
                    "fundingRate": item.get("fundingRate", ""),
                }
            )
        oldest_ts = min(int(item["fundingTime"]) for item in items)
        if reached_start or oldest_ts < start_ms:
            break
        after = str(oldest_ts)
    return rows


def cex_zip_bytes(content: bytes):
    """包装Zip字节为内存文件。"""
    return BytesIO(content)


def build_rows(exchange: str, symbol: str, start_date: str, existing_ts: set) -> list:
    """构造指定交易所的新增资金费率行。"""
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    if exchange == "bybit":
        raw_rows = fetch_bybit_rows(symbol, start_ms, end_ms)
    elif exchange == "binance":
        raw_rows = fetch_binance_bucket_rows(symbol, start_date)
        raw_rows.extend(fetch_binance_api_rows(symbol, max(start_ms, int(datetime.now(tz=timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)), end_ms))
    elif exchange == "bitget":
        raw_rows = fetch_bitget_rows(symbol, start_ms)
    elif exchange == "okx":
        raw_rows = fetch_okx_rows(symbol, start_ms)
    else:
        raw_rows = []
    rows = []
    for item in raw_rows:
        ts_ms = int(item["fundingRateTimestamp"])
        if ts_ms in existing_ts:
            continue
        rows.append(
            {
                "symbol": item["symbol"],
                "fundingRateTimestamp": str(ts_ms),
                "datetime": utc_datetime_str(ts_ms),
                "date": utc_date_str(ts_ms),
                "fundingRate": item["fundingRate"],
            }
        )
    rows.sort(key=lambda item: int(item["fundingRateTimestamp"]))
    return rows


def write_rows(file_path: Path, rows: list) -> int:
    """写入资金费率CSV文件。"""
    if not rows:
        return 0
    file_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not file_path.exists()
    with file_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["symbol", "fundingRateTimestamp", "datetime", "date", "fundingRate"],
        )
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    upload_file_to_s3(file_path)
    return len(rows)


def run_symbol(exchange: str, symbol: str) -> None:
    """执行单个交易对的资金费率同步。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    start_date = cex_config.get_start_date(DATASET_ID, exchange, symbol)
    if not base_dir or not start_date:
        status_update(exchange, "future", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    file_path = build_file_path(base_dir, symbol)
    existing_ts = load_existing_timestamps(file_path)
    latest_existing_date = latest_date_from_timestamps(existing_ts)
    if latest_existing_date:
        status_update(exchange, "future", symbol, (len(existing_ts), f"日 {latest_existing_date} 准备同步"))
    else:
        status_update(exchange, "future", symbol, (len(existing_ts), f"准备 {start_date}"))
    log(f"{exchange} {symbol} 开始同步: {latest_existing_date or start_date} -> {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d')}")
    rows = build_rows(exchange, symbol, start_date, existing_ts)
    count = write_rows(file_path, rows)
    latest_synced_date = rows[-1]["date"] if rows else latest_existing_date
    if latest_synced_date:
        status_update(exchange, "future", symbol, (len(existing_ts) + count, f"日 {latest_synced_date} 已完成"))
    else:
        status_update(exchange, "future", symbol, (len(existing_ts) + count, "无可用数据"))
    log(f"{exchange} {symbol} 已写入记录数: {count}")


def run_exchange(exchange: str) -> None:
    """执行单个交易所的资金费率同步。"""
    if not cex_config.is_supported(DATASET_ID, exchange):
        for symbol in cex_config.get_funding_symbols(exchange):
            status_update(exchange, "future", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    for symbol in cex_config.get_funding_symbols(exchange):
        run_symbol(exchange, symbol)


def main() -> None:
    """运行资金费率主循环。"""
    while True:
        threads = []
        for exchange in cex_config.list_exchanges():
            thread = threading.Thread(target=run_exchange, args=(exchange,), daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join()
        sleep_seconds = seconds_until_next_utc_midnight()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 00:00）")
        status_update("system", "future", "funding", f"等待 {sleep_seconds}s")
        threading.Event().wait(sleep_seconds)


def run() -> None:
    """兼容启动器运行入口。"""
    main()


if __name__ == "__main__":
    run()
