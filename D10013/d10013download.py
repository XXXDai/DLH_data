from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import shutil
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
from tqdm import tqdm
import app_config

BASE_URL = "https://public.bybit.com/trading"  # 下载根地址，字符串
INSTRUMENTS_BASE_URL = "https://api.bybit.com/v5/market/instruments-info"  # 交易对接口地址，字符串
INSTRUMENTS_TIMEOUT_SECONDS = 10  # 交易对接口超时，秒
INSTRUMENTS_LIMIT = 1000  # 交易对接口分页大小，条
DELIVERY_CATEGORIES = app_config.BYBIT_FUTURE_DELIVERY_CATEGORIES  # 交割期货产品类型列表，个数
DELIVERY_STATUSES = app_config.BYBIT_FUTURE_DELIVERY_STATUSES  # 交割期货状态列表，个数
START_DATE = app_config.D10013_START_DATE  # 起始日期（含），日期
DATA_DIR = Path("data/src/bybit_future_trade_di")  # 保存目录，路径
TIMEOUT_SECONDS = 30  # 请求超时，秒
RETRY_TIMES = 5  # 最大重试次数，次
RETRY_INTERVAL_SECONDS = 5  # 重试间隔，秒
CHUNK_SIZE = 1024 * 1024  # 下载块大小，字节
LOOP_INTERVAL_SECONDS = 4 * 60 * 60  # 循环间隔，秒
FAIL_LOG_DIR = Path("D10013")  # 失败记录目录，路径
QUIET = False  # 静默模式开关，开关


def log(message: str) -> None:
    if not QUIET:
        print(message)


def build_fail_log_path() -> Path:
    return FAIL_LOG_DIR / "download_failures.json"


def build_url(base_url: str, symbol: str, date_str: str) -> str:
    return f"{base_url}/{symbol}/{symbol}{date_str}.csv.gz"


def build_output_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    file_name = f"{symbol}{date_str}.csv.gz"
    return base_dir / symbol / file_name


def iter_dates(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def request_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=INSTRUMENTS_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("接口请求失败: 网络错误") from exc


def build_instruments_url(category: str, status: str, cursor: str | None) -> str:
    params = {
        "category": category,
        "status": status,
        "limit": INSTRUMENTS_LIMIT,
    }
    if cursor:
        params["cursor"] = cursor
    return f"{INSTRUMENTS_BASE_URL}?{urlencode(params)}"


def list_delivery_symbols(start_date: str) -> list:
    start_ts = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(start_ts.timestamp() * 1000)
    symbols = set()
    for category in DELIVERY_CATEGORIES:
        for status in DELIVERY_STATUSES:
            cursor = None
            while True:
                url = build_instruments_url(category, status, cursor)
                payload = request_json(url)
                if payload.get("retCode") != 0:
                    raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
                result = payload.get("result", {})
                items = result.get("list", [])
                for item in items:
                    contract_type = item.get("contractType", "")
                    if "Futures" not in contract_type:
                        continue
                    delivery_time = int(item.get("deliveryTime", "0") or 0)
                    if delivery_time == 0:
                        continue
                    if delivery_time < start_ms:
                        continue
                    symbol = item.get("symbol")
                    if symbol:
                        symbols.add(symbol)
                cursor = result.get("nextPageCursor")
                if not cursor:
                    break
    return sorted(symbols)

def download_file(url: str, dest_path: Path) -> tuple[int | None, str | None]:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
                length = response.getheader("Content-Length")
                total_size = int(length) if length and length.isdigit() else None
                with dest_path.open("wb") as f:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc="下载",
                        disable=QUIET,
                    ) as pbar:
                        while True:
                            chunk = response.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            f.write(chunk)
                            pbar.update(len(chunk))
            return dest_path.stat().st_size, None
        except HTTPError as exc:
            last_error = f"HTTP错误: {exc}"
            if exc.code == 404:
                log(f"下载失败，HTTP错误404，不重试: {exc}")
                if dest_path.exists():
                    dest_path.unlink()
                return None, last_error
            log(f"下载失败，HTTP错误，重试{attempt}/{RETRY_TIMES}: {exc}")
            if dest_path.exists():
                dest_path.unlink()
        except URLError as exc:
            last_error = f"网络错误: {exc}"
            log(f"下载失败，网络错误，重试{attempt}/{RETRY_TIMES}: {exc}")
            if dest_path.exists():
                dest_path.unlink()
        if attempt < RETRY_TIMES:
            time.sleep(RETRY_INTERVAL_SECONDS)
    return None, last_error


def load_failures(path: Path) -> list:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def save_failures(path: Path, failures: list) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(failures, f, ensure_ascii=False, indent=2)


def upsert_failure(failures: list, record: dict) -> None:
    for idx, item in enumerate(failures):
        if item.get("url") == record.get("url"):
            failures[idx].update(record)
            return
    failures.append(record)


def remove_failure(failures: list, url: str) -> None:
    failures[:] = [item for item in failures if item.get("url") != url]


def has_failure(failures: list, url: str) -> bool:
    return any(item.get("url") == url for item in failures)


def download_by_url(url: str, date_str: str, symbol: str, failures: list) -> None:
    dest_path = build_output_path(DATA_DIR, symbol, date_str)
    size, error_message = download_file(url, dest_path)
    if size is None:
        upsert_failure(
            failures,
            {
                "日期": date_str,
                "交易对": symbol,
                "url": url,
                "错误信息": error_message,
                "重试次数": RETRY_TIMES,
                "记录时间": datetime.now().isoformat(),
            },
        )
        log(f"下载失败已记录: {date_str}")
        return
    remove_failure(failures, url)
    log(f"已下载: {dest_path}，大小: {size} 字节")


def run_initial_range(symbol: str, failures: list, fail_path: Path) -> set:
    end_date = datetime.now().strftime("%Y-%m-%d")
    dates = list(iter_dates(START_DATE, end_date))
    total_days = len(dates)
    log(f"总天数: {total_days}")
    attempted = set()
    for idx, date_str in enumerate(dates, 1):
        log(f"日期进度: {idx}/{total_days} {date_str}")
        url = build_url(BASE_URL, symbol, date_str)
        attempted.add(url)
        download_by_url(url, date_str, symbol, failures)
        save_failures(fail_path, failures)
    return attempted


def retry_failures(failures: list, skip_urls: set, symbol: str, fail_path: Path) -> None:
    if not failures:
        return
    log(f"失败重试数: {len(failures)}")
    for item in list(failures):
        url = item.get("url")
        if not url:
            continue
        if url in skip_urls:
            continue
        date_str = item.get("日期") or "未知日期"
        download_by_url(url, date_str, symbol, failures)
        save_failures(fail_path, failures)


def download_today(failures: list, skip_urls: set, symbol: str, fail_path: Path) -> None:
    today = datetime.now().strftime("%Y-%m-%d")
    log(f"当前日期下载: {today}")
    url = build_url(BASE_URL, symbol, today)
    if url in skip_urls:
        log("今日数据已在本轮尝试中，跳过重复请求")
        return
    if has_failure(failures, url):
        log("今日数据已在失败列表中，本轮跳过重复请求")
        return
    dest_path = build_output_path(DATA_DIR, symbol, today)
    if dest_path.exists():
        log(f"已存在，跳过今日下载: {dest_path}")
        remove_failure(failures, url)
        return
    download_by_url(url, today, symbol, failures)
    save_failures(fail_path, failures)


def run_symbol(symbol: str) -> None:
    fail_path = build_fail_log_path()
    failures = load_failures(fail_path)
    skip_urls = run_initial_range(symbol, failures, fail_path)
    while True:
        failures = load_failures(fail_path)
        retry_failures(failures, skip_urls, symbol, fail_path)
        download_today(failures, skip_urls, symbol, fail_path)
        skip_urls = set()
        log(f"等待 {LOOP_INTERVAL_SECONDS} 秒后再次执行")
        time.sleep(LOOP_INTERVAL_SECONDS)


def main() -> None:
    threads = []
    symbols = list_delivery_symbols(START_DATE)
    if not symbols:
        log("未获取到交割期货交易对")
        return
    for symbol in symbols:
        thread = threading.Thread(target=run_symbol, args=(symbol,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


if __name__ == "__main__":
    run()
