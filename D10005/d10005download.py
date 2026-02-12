from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import importlib
import shutil
import threading
import time
import sys
import zipfile
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import urlopen
from tqdm import tqdm

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

BASE_URL = "https://quote-saver.bycsi.com/orderbook/spot"  # 下载根地址，字符串
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
START_DATE = app_config.D10005_START_DATE  # 起始日期（不含），日期
DATA_DIR = Path("data/src/bybit_spot_orderbook_di")  # 保存目录，路径
TIMEOUT_SECONDS = 30  # 请求超时，秒
RETRY_TIMES = 5  # 最大重试次数，次
RETRY_INTERVAL_SECONDS = 5  # 重试间隔，秒
CHUNK_SIZE = 1024 * 1024  # 下载块大小，字节
LOOP_INTERVAL_SECONDS = 4 * 60 * 60  # 循环间隔，秒
INITIAL_BACKFILL_DAYS = 7  # 首次回填天数，天
FAIL_LOG_DIR = Path("D10005")  # 失败记录目录，路径
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数
FAIL_LOCK = threading.Lock()  # 失败记录锁，锁


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def seconds_until_next_utc_midnight() -> int:
    now = datetime.now(tz=timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = int((next_midnight - now).total_seconds())
    return seconds if seconds > 0 else 1


def build_fail_log_path() -> Path:
    return FAIL_LOG_DIR / "download_failures.json"


def resolve_output_path(url: str, base_dir: Path) -> Path:
    file_name = Path(urlparse(url).path).name
    parts = file_name.split("_")
    symbol = parts[1] if len(parts) > 1 else "UNKNOWN"
    return base_dir / symbol / file_name


def extract_symbol_from_url(url: str) -> str | None:
    parts = [item for item in urlparse(url).path.split("/") if item]
    return parts[-2] if len(parts) >= 2 else None


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


def download_file(url: str, dest_path: Path) -> tuple[int | None, str | None]:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = dest_path.with_name(dest_path.name + ".part")
    last_error = None
    for attempt in range(1, RETRY_TIMES + 1):
        try:
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
            time.sleep(RETRY_INTERVAL_SECONDS)
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


def download_by_url(url: str, date_str: str, symbol: str, fail_path: Path, failures: list) -> None:
    dest_path = resolve_output_path(url, DATA_DIR)
    if dest_path.exists() and zipfile.is_zipfile(dest_path):
        failures[:] = update_failures_file(fail_path, None, url)
        log(f"已存在，跳过下载: {dest_path}")
        return
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
        return
    failures[:] = update_failures_file(fail_path, None, url)
    log(f"已下载: {dest_path}，大小: {size} 字节")


def run_initial_range(start_date: str, symbol: str, failures: list, fail_path: Path) -> set:
    end_date = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    dates = list(iter_dates(start_date, end_date))
    total_days = len(dates)
    log(f"总天数: {total_days}")
    attempted = set()
    for idx, date_str in enumerate(dates, 1):
        log(f"日期进度: {idx}/{total_days} {date_str}")
        url = build_url(BASE_URL, symbol, date_str)
        attempted.add(url)
        download_by_url(url, date_str, symbol, fail_path, failures)
    return attempted


def retry_failures(failures: list, skip_urls: set, symbol: str, fail_path: Path) -> None:
    if not failures:
        return
    log(f"失败重试数: {len(failures)}")
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
        download_by_url(url, date_str, symbol, fail_path, failures)


def download_today(failures: list, skip_urls: set, symbol: str, fail_path: Path) -> None:
    date_str = (datetime.now(tz=timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
    log(f"当前日期下载: {date_str}（UTC）")
    url = build_url(BASE_URL, symbol, date_str)
    if url in skip_urls:
        log("今日数据已在本轮尝试中，跳过重复请求")
        return
    if has_failure(failures, url):
        log("今日数据已在失败列表中，本轮跳过重复请求")
        return
    dest_path = resolve_output_path(url, DATA_DIR)
    if dest_path.exists():
        if not zipfile.is_zipfile(dest_path):
            log(f"文件不是有效zip，将重新下载: {dest_path}")
            dest_path.unlink()
        else:
            log(f"已存在，跳过今日下载: {dest_path}")
            failures[:] = update_failures_file(fail_path, None, url)
            return
    download_by_url(url, date_str, symbol, fail_path, failures)


def run_symbol(symbol: str) -> None:
    fail_path = build_fail_log_path()
    failures = load_failures(fail_path)
    latest_local = get_latest_local_date(symbol)
    if latest_local:
        start_date = latest_local
    else:
        backfill_start = (datetime.now(tz=timezone.utc) - timedelta(days=INITIAL_BACKFILL_DAYS)).strftime("%Y-%m-%d")
        start_date = max(START_DATE, backfill_start)
    skip_urls = run_initial_range(start_date, symbol, failures, fail_path)
    while True:
        failures = load_failures(fail_path)
        retry_failures(failures, skip_urls, symbol, fail_path)
        download_today(failures, skip_urls, symbol, fail_path)
        skip_urls = set()
        sleep_seconds = seconds_until_next_utc_midnight()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 00:00）")
        time.sleep(sleep_seconds)


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
