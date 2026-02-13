from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import json
import importlib
import shutil
import socket
import threading
import time
import sys
import traceback
import zipfile
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import urlopen
from tqdm import tqdm

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

BASE_URL = "https://quote-saver.bycsi.com/orderbook/linear"  # 下载根地址，字符串
INSTRUMENTS_BASE_URL = "https://api.bybit.com/v5/market/instruments-info"  # 交易对接口地址，字符串
INSTRUMENTS_TIMEOUT_SECONDS = app_config.INSTRUMENTS_TIMEOUT_SECONDS  # 交易对接口超时，秒
INSTRUMENTS_LIMIT = app_config.INSTRUMENTS_LIMIT  # 交易对接口分页大小，条
DELIVERY_CATEGORIES = app_config.BYBIT_FUTURE_DELIVERY_CATEGORIES  # 交割期货产品类型列表，个数
DELIVERY_STATUSES = app_config.BYBIT_FUTURE_DELIVERY_STATUSES  # 交割期货状态列表，个数
DELIVERY_EXCLUDE = app_config.BYBIT_FUTURE_DELIVERY_EXCLUDE  # 交割合约过滤列表，个数
DELIVERY_SYMBOL_PATTERN = re.compile(r".+-\d{2}[A-Z]{3}\d{2}$")  # 交割合约格式，正则
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
START_DATE = app_config.D10001_START_DATE  # 起始日期（不含），日期
DATA_DIR = Path("data/src/bybit_future_orderbook_di")  # 保存目录，路径
TIMEOUT_SECONDS = app_config.DOWNLOAD_TIMEOUT_SECONDS  # 请求超时，秒
RETRY_TIMES = app_config.RETRY_TIMES  # 最大重试次数，次
RETRY_INTERVAL_SECONDS = app_config.RETRY_INTERVAL_SECONDS  # 重试间隔，秒
CHUNK_SIZE = app_config.CHUNK_SIZE  # 下载块大小，字节
DOWNLOAD_CONCURRENCY = app_config.DOWNLOAD_CONCURRENCY  # 下载并发数，个数
DOWNLOAD_SEMAPHORE = threading.Semaphore(DOWNLOAD_CONCURRENCY)  # 下载并发信号量，个数
LOOP_INTERVAL_SECONDS = app_config.LOOP_INTERVAL_SECONDS  # 循环间隔，秒
DELIVERY_REFRESH_SECONDS = app_config.DELIVERY_REFRESH_SECONDS  # 交割合约刷新间隔，秒
FAIL_LOG_DIR = Path("D10001")  # 失败记录目录，路径
QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数
FAIL_LOCK = threading.Lock()  # 失败记录锁，锁
ALIGN_LOCK = threading.Lock()  # 启动对齐锁，锁
ALIGN_LOGGED = False  # 启动对齐日志标记，开关
DELIVERY_LOCK = threading.Lock()  # 交割合约锁，锁
DELIVERY_TIMES = {}  # 交割合约到期映射，毫秒
RUNNING_LOCK = threading.Lock()  # 运行线程锁，锁
RUNNING_SYMBOLS = set()  # 运行中交易对集合，个数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)

def status_update(symbol: str, done_count: int, text: str) -> None:
    if STATUS_HOOK:
        STATUS_HOOK(symbol, (done_count, text))


def seconds_until_next_utc_midnight() -> int:
    now = datetime.now(tz=timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = int((next_midnight - now).total_seconds())
    return seconds if seconds > 0 else 1


def seconds_until_next_utc_4h() -> int:
    now = datetime.now(tz=timezone.utc)
    hour_block = (now.hour // 4 + 1) * 4
    if hour_block >= 24:
        next_dt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        next_dt = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)
    seconds = int((next_dt - now).total_seconds())
    return seconds if seconds > 0 else 1


def align_to_utc_4h_boundary(symbol: str, done_count: int) -> None:
    now = datetime.now(tz=timezone.utc)
    if now.hour % 4 == 0 and now.minute == 0 and now.second == 0:
        status_update(symbol, done_count, "已对齐")
        return
    sleep_seconds = seconds_until_next_utc_4h()
    status_update(symbol, done_count, f"对齐等待 {sleep_seconds}s")
    global ALIGN_LOGGED
    with ALIGN_LOCK:
        if not ALIGN_LOGGED:
            log(f"启动对齐，等待 {sleep_seconds} 秒后执行（UTC 4小时倍数）")
            ALIGN_LOGGED = True
    time.sleep(sleep_seconds)
    status_update(symbol, done_count, "已对齐")


def build_fail_log_path() -> Path:
    return FAIL_LOG_DIR / "download_failures.json"


def resolve_output_path(url: str, base_dir: Path) -> Path:
    file_name = Path(urlparse(url).path).name
    parts = file_name.split("_")
    symbol = parts[1] if len(parts) > 1 else "UNKNOWN"
    return base_dir / symbol / file_name


def build_url(base_url: str, symbol: str, date_str: str) -> str:
    return f"{base_url}/{symbol}/{date_str}_{symbol}_ob200.data.zip"


def parse_date_from_name(name: str) -> str | None:
    parts = name.split("_", 1)
    if not parts:
        return None
    date_str = parts[0]
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    return date_str


def get_latest_local_date(symbol: str) -> str | None:
    symbol_dir = DATA_DIR / symbol
    if not symbol_dir.exists():
        return None
    latest = None
    for path in symbol_dir.glob("*_ob200.data.zip"):
        date_str = parse_date_from_name(path.name)
        if not date_str:
            continue
        latest = date_str if latest is None else max(latest, date_str)
    return latest


def iter_dates(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start + timedelta(days=1)
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def request_json(url: str) -> dict | None:
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            with urlopen(url, timeout=INSTRUMENTS_TIMEOUT_SECONDS) as response:
                payload = response.read().decode("utf-8")
            return json.loads(payload)
        except (TimeoutError, socket.timeout, HTTPError, URLError, json.JSONDecodeError) as exc:
            log(f"接口请求失败，重试 {attempt}/{RETRY_TIMES}: {url} {exc}")
            log(traceback.format_exc().strip())
            time.sleep(RETRY_INTERVAL_SECONDS)
    log(f"接口请求失败，放弃本轮: {url}")
    return None


def build_instruments_url(category: str, status: str, cursor: str | None) -> str:
    params = {
        "category": category,
        "status": status,
        "limit": INSTRUMENTS_LIMIT,
    }
    if cursor:
        params["cursor"] = cursor
    return f"{INSTRUMENTS_BASE_URL}?{urlencode(params)}"


def list_delivery_symbols(start_date: str) -> dict:
    start_ts = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    start_ms = int(start_ts.timestamp() * 1000)
    symbols = {}
    for category in DELIVERY_CATEGORIES:
        for status in DELIVERY_STATUSES:
            cursor = None
            while True:
                url = build_instruments_url(category, status, cursor)
                payload = request_json(url)
                if not payload:
                    break
                if payload.get("retCode") != 0:
                    log(f"接口返回错误: {payload.get('retMsg')}")
                    break
                result = payload.get("result", {})
                items = result.get("list", [])
                for item in items:
                    contract_type = item.get("contractType", "")
                    if "Futures" not in contract_type and "Perpetual" not in contract_type:
                        continue
                    delivery_time = int(item.get("deliveryTime", "0") or 0)
                    if delivery_time == 0:
                        continue
                    if delivery_time != 0 and delivery_time < start_ms:
                        continue
                    symbol = item.get("symbol")
                    if symbol and not DELIVERY_SYMBOL_PATTERN.match(symbol):
                        continue
                    if symbol and not symbol.split("-")[0].endswith("USDT"):
                        continue
                    base_symbol = symbol.split("-")[0] if symbol else ""
                    if symbol and base_symbol not in DELIVERY_EXCLUDE:
                        symbols[symbol] = delivery_time
                cursor = result.get("nextPageCursor")
                if not cursor:
                    break
    return symbols


def set_delivery_times(times: dict) -> None:
    global DELIVERY_TIMES
    with DELIVERY_LOCK:
        DELIVERY_TIMES = dict(times)


def get_delivery_time(symbol: str) -> int | None:
    with DELIVERY_LOCK:
        return DELIVERY_TIMES.get(symbol)


def get_delivery_end_date(symbol: str) -> str | None:
    delivery_time = get_delivery_time(symbol)
    if not delivery_time:
        return None
    return datetime.fromtimestamp(delivery_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def is_delivery_complete(symbol: str, delivery_end: str | None) -> bool:
    if not delivery_end:
        return False
    now_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    if now_date <= delivery_end:
        return False
    latest_local = get_latest_local_date(symbol)
    return bool(latest_local and latest_local >= delivery_end)


def extract_symbol_from_url(url: str) -> str | None:
    parts = [item for item in urlparse(url).path.split("/") if item]
    return parts[-2] if len(parts) >= 2 else None


def download_file(url: str, dest_path: Path) -> tuple[int | None, str | None]:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_name(dest_path.name + ".part")
    last_error = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            with DOWNLOAD_SEMAPHORE:
                with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
                    length = response.getheader("Content-Length")
                    total_size = int(length) if length and length.isdigit() else None
                    if tmp_path.exists():
                        tmp_path.unlink()
                    with tmp_path.open("wb") as f:
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
            downloaded_size = tmp_path.stat().st_size
            if downloaded_size == 0:
                last_error = "下载为空: 0字节"
                log(f"下载失败，下载为空，重试{attempt}/{RETRY_TIMES}: 0字节")
                tmp_path.unlink()
                continue
            if total_size is not None and downloaded_size != total_size:
                last_error = f"下载不完整: {downloaded_size}/{total_size}"
                log(f"下载失败，下载不完整，重试{attempt}/{RETRY_TIMES}: {downloaded_size}/{total_size}")
                tmp_path.unlink()
                continue
            tmp_path.replace(dest_path)
            return downloaded_size, None
        except (TimeoutError, socket.timeout) as exc:
            last_error = f"超时错误: {exc}"
            log(f"下载失败，超时错误，重试{attempt}/{RETRY_TIMES}: {exc}")
            if tmp_path.exists():
                tmp_path.unlink()
        except HTTPError as exc:
            last_error = f"HTTP错误: {exc}"
            if exc.code == 404:
                log(f"下载失败，HTTP错误404，不重试: {exc}")
                if tmp_path.exists():
                    tmp_path.unlink()
                return None, last_error
            log(f"下载失败，HTTP错误，重试{attempt}/{RETRY_TIMES}: {exc}")
            if tmp_path.exists():
                tmp_path.unlink()
        except URLError as exc:
            last_error = f"网络错误: {exc}"
            log(f"下载失败，网络错误，重试{attempt}/{RETRY_TIMES}: {exc}")
            if tmp_path.exists():
                tmp_path.unlink()
        if attempt < RETRY_TIMES:
            backoff = RETRY_INTERVAL_SECONDS * (2 ** (attempt - 1))
            jitter = time.time() % 1
            time.sleep(min(60, backoff) + jitter)
    return None, last_error


def load_failures(path: Path) -> list:
    with FAIL_LOCK:
        return load_failures_unlocked(path)


def load_failures_unlocked(path: Path) -> list:
    if not path.exists():
        return []
    if path.stat().st_size == 0:
        return []
    text = path.read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []
    if not stripped.startswith("[") or not stripped.endswith("]"):
        return []
    data = json.loads(stripped)
    return data if isinstance(data, list) else []


def save_failures(path: Path, failures: list) -> None:
    with FAIL_LOCK:
        save_failures_unlocked(path, failures)


def save_failures_unlocked(path: Path, failures: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


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


def update_failures_file(path: Path, record: dict | None, url: str) -> list:
    with FAIL_LOCK:
        failures = load_failures_unlocked(path)
        if record is None:
            remove_failure(failures, url)
        else:
            upsert_failure(failures, record)
        save_failures_unlocked(path, failures)
        return failures


def download_by_url(url: str, date_str: str, symbol: str, fail_path: Path, failures: list) -> bool:
    dest_path = resolve_output_path(url, DATA_DIR)
    if dest_path.exists() and zipfile.is_zipfile(dest_path):
        failures[:] = update_failures_file(fail_path, None, url)
        log(f"已存在，跳过下载: {dest_path}")
        return True
    size, error_message = download_file(url, dest_path)
    if size is None:
        record = {
            "日期": date_str,
            "交易对": symbol,
            "url": url,
            "错误信息": error_message,
            "重试次数": RETRY_TIMES,
            "记录时间": datetime.now().isoformat(),
        }
        failures[:] = update_failures_file(fail_path, record, url)
        log(f"下载失败已记录: {date_str}")
        return False
    failures[:] = update_failures_file(fail_path, None, url)
    log(f"已下载: {dest_path}，大小: {size} 字节")
    return True


def run_initial_range(
    start_date: str,
    symbol: str,
    failures: list,
    fail_path: Path,
    done_count: int,
    delivery_end: str | None,
) -> tuple[set, int]:
    end_date = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    if delivery_end and delivery_end < end_date:
        end_date = delivery_end
    dates = list(iter_dates(start_date, end_date))
    total_days = len(dates)
    log(f"{symbol} 总天数: {total_days}")
    attempted = set()
    for idx, date_str in enumerate(dates, 1):
        url = build_url(BASE_URL, symbol, date_str)
        file_name = Path(urlparse(url).path).name
        log(f"{symbol} 日期进度: {idx}/{total_days} {date_str} 正在获取: {file_name}")
        status_update(symbol, done_count, f"{idx}/{total_days} {date_str} {file_name}")
        attempted.add(url)
        if download_by_url(url, date_str, symbol, fail_path, failures):
            done_count += 1
            status_update(symbol, done_count, f"{idx}/{total_days} {date_str} {file_name}")
    return attempted, done_count


def retry_failures(
    failures: list,
    skip_urls: set,
    symbol: str,
    fail_path: Path,
    done_count: int,
    delivery_end: str | None,
) -> int:
    if not failures:
        return done_count
    log(f"{symbol} 失败重试数: {len(failures)}")
    for item in list(failures):
        url = item.get("url")
        if not url:
            continue
        url_symbol = extract_symbol_from_url(url)
        if url_symbol and url_symbol != symbol:
            continue
        if url in skip_urls:
            continue
        date_str = item.get("日期") or "未知日期"
        if delivery_end and date_str != "未知日期" and date_str > delivery_end:
            continue
        file_name = Path(urlparse(url).path).name
        status_update(symbol, done_count, f"重试 {date_str} {file_name}")
        if download_by_url(url, date_str, symbol, fail_path, failures):
            done_count += 1
            status_update(symbol, done_count, f"重试 {date_str} {file_name}")
    return done_count


def download_today(
    failures: list,
    skip_urls: set,
    symbol: str,
    fail_path: Path,
    done_count: int,
    delivery_end: str | None,
) -> int:
    date_str = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    if delivery_end and date_str > delivery_end:
        log(f"交割合约已到期，停止今日下载: {symbol} {delivery_end}")
        return done_count
    log(f"{symbol} 当前日期下载: {date_str}（UTC）")
    url = build_url(BASE_URL, symbol, date_str)
    file_name = Path(urlparse(url).path).name
    status_update(symbol, done_count, f"今日 {date_str} {file_name}")
    if url in skip_urls:
        log(f"{symbol} 今日数据已在本轮尝试中，跳过重复请求")
        return done_count
    if has_failure(failures, url):
        log(f"{symbol} 今日数据已在失败列表中，本轮跳过重复请求")
        return done_count
    dest_path = resolve_output_path(url, DATA_DIR)
    if dest_path.exists():
        if not zipfile.is_zipfile(dest_path):
            log(f"文件不是有效zip，将重新下载: {dest_path}")
            dest_path.unlink()
        else:
            log(f"{symbol} 已存在，跳过今日下载: {dest_path}")
            failures[:] = update_failures_file(fail_path, None, url)
            return done_count
    if download_by_url(url, date_str, symbol, fail_path, failures):
        done_count += 1
        status_update(symbol, done_count, f"今日 {date_str} {file_name}")
    return done_count


def run_symbol(symbol: str) -> None:
    fail_path = build_fail_log_path()
    failures = load_failures(fail_path)
    delivery_end = get_delivery_end_date(symbol)
    latest_local = get_latest_local_date(symbol)
    if latest_local:
        start_date = latest_local
    else:
        start_date = START_DATE
    done_count = 0
    skip_urls, done_count = run_initial_range(
        start_date, symbol, failures, fail_path, done_count, delivery_end
    )
    if is_delivery_complete(symbol, delivery_end):
        log(f"交割合约已到期，停止循环: {symbol} {delivery_end}")
        return
    while True:
        failures = load_failures(fail_path)
        delivery_end = get_delivery_end_date(symbol)
        done_count = retry_failures(
            failures, skip_urls, symbol, fail_path, done_count, delivery_end
        )
        done_count = download_today(
            failures, skip_urls, symbol, fail_path, done_count, delivery_end
        )
        if is_delivery_complete(symbol, delivery_end):
            log(f"交割合约已到期，停止循环: {symbol} {delivery_end}")
            return
        skip_urls = set()
        sleep_seconds = seconds_until_next_utc_4h()
        log(f"{symbol} 等待 {sleep_seconds} 秒后再次执行（UTC 4小时倍数）")
        time.sleep(sleep_seconds)


def start_symbol_thread(symbol: str, threads: list) -> None:
    thread = threading.Thread(target=run_symbol, args=(symbol,))
    thread.start()
    threads.append(thread)
    RUNNING_SYMBOLS.add(symbol)


def refresh_delivery_loop(start_date: str, threads: list) -> None:
    while True:
        try:
            delivery_map = list_delivery_symbols(start_date)
        except HTTPError as exc:
            log(f"交割合约获取失败，HTTP错误: {exc}")
            time.sleep(DELIVERY_REFRESH_SECONDS)
            continue
        except URLError as exc:
            log(f"交割合约获取失败，网络错误: {exc}")
            time.sleep(DELIVERY_REFRESH_SECONDS)
            continue
        set_delivery_times(delivery_map)
        with RUNNING_LOCK:
            new_symbols = sorted(set(delivery_map) - RUNNING_SYMBOLS)
            for symbol in new_symbols:
                start_symbol_thread(symbol, threads)
        time.sleep(DELIVERY_REFRESH_SECONDS)


def main() -> None:
    delivery_map = {}
    try:
        delivery_map = list_delivery_symbols(START_DATE)
    except HTTPError as exc:
        log(f"交割合约获取失败，HTTP错误: {exc}")
    except URLError as exc:
        log(f"交割合约获取失败，网络错误: {exc}")
    set_delivery_times(delivery_map)
    symbols = sorted(set(SYMBOLS) | set(delivery_map))
    if not symbols:
        log("未配置交易对")
        return
    threads = []
    with RUNNING_LOCK:
        for symbol in symbols:
            start_symbol_thread(symbol, threads)
    refresh_thread = threading.Thread(target=refresh_delivery_loop, args=(START_DATE, threads))
    refresh_thread.start()
    threads.append(refresh_thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


if __name__ == "__main__":
    run()
