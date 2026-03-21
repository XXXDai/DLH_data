from collections import defaultdict
from datetime import datetime, timedelta, timezone
from io import BytesIO, TextIOWrapper
from pathlib import Path
import csv
import gzip
import json
import re
import socket
import sys
import threading
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen

import app_config
from cex import cex_config
from cex.cex_common import download_bytes
from cex.cex_common import count_existing_days
from cex.cex_common import get_synced_until_date
from cex.cex_common import is_valid_gzip_file
from cex.cex_common import iter_dates
from cex.cex_common import iter_months
from cex.cex_common import list_missing_dates
from cex.cex_common import list_storage_file_names
from cex.cex_common import month_end
from cex.cex_common import replace_output_file
from cex.cex_common import seconds_until_next_utc_4h
from cex.cex_common import update_failure_file
from cex.cex_common import write_gzip_csv_rows
from cex.cex_orderbook_ws_common import NetworkRequestError
from cex.cex_orderbook_ws_common import list_bybit_delivery_symbol_start_dates
from cex.cex_orderbook_ws_common import list_bybit_delivery_symbols_since
from cex.cex_trade_common import FUTURE_TRADE_FIELDS
from cex.cex_trade_common import normalize_bitget_future_trade_row
from cex.cex_trade_common import normalize_binance_future_parts
from cex.cex_trade_common import normalize_okx_future_trade_row


DATASET_ID = "D10013"  # 数据集标识，字符串
FAIL_LOG_DIR = Path("D10013")  # 失败日志目录，路径
TIMEOUT_SECONDS = app_config.DOWNLOAD_TIMEOUT_SECONDS  # 请求超时，秒
BYBIT_BASE_URL = "https://public.bybit.com/trading"  # Bybit期货成交根地址，字符串
BINANCE_BUCKET_URL = "https://data.binance.vision"  # Binance公共数据根地址，字符串
BITGET_FUTURE_BASE_URL = "https://img.bitgetimg.com/online/trades/UMCBL"  # Bitget期货成交根地址，字符串
OKX_MARKET_HISTORY_URL = "https://www.okx.com/api/v5/public/market-data-history"  # OKX历史市场数据接口地址，字符串
OKX_MODULE_1_DATE_TZ_OFFSET_HOURS = 8  # OKX模块1时间偏移，小时
HTTP_HEADER_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"  # 通用请求头浏览器标识，字符串
HTTP_HEADER_ACCEPT_JSON = "application/json, text/plain, */*"  # JSON请求头可接受类型，字符串
HTTP_HEADER_ACCEPT_ALL = "*/*"  # 文件请求头可接受类型，字符串
HTTP_HEADER_ACCEPT_LANGUAGE = "en-US,en;q=0.9"  # 请求头语言偏好，字符串
HTTP_HEADER_BITGET_REFERER = "https://www.bitget.com/"  # Bitget请求头来源地址，字符串
HTTP_HEADER_BITGET_ORIGIN = "https://www.bitget.com"  # Bitget请求头来源域名，字符串
HTTP_HEADER_OKX_REFERER = "https://www.okx.com/"  # OKX请求头来源地址，字符串
HTTP_HEADER_OKX_ORIGIN = "https://www.okx.com"  # OKX请求头来源域名，字符串
REQUEST_MIN_INTERVAL_SECONDS = 0.2  # 接口最小请求间隔，秒
BITGET_MAX_FILE_INDEX = 256  # Bitget单日最大分片数，个
BITGET_ARCHIVE_NAME_PATTERN = re.compile(r"^(\d{8})_\d{3}\.zip$")  # Bitget归档文件名格式，正则
QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    """输出日志消息。"""
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def status_update(exchange: str, symbol: str, value) -> None:
    """更新期货成交状态。"""
    if STATUS_HOOK:
        STATUS_HOOK(cex_config.get_status_key(exchange, "future", symbol), value)


def update_memory_metrics(exchange: str, phase: str, archive_bytes: int, grouped_bytes: int, row_count: int, group_count: int) -> None:
    """更新期货成交下载的内存观测。"""
    cex_config.update_runtime_memory_metrics(
        DATASET_ID,
        exchange,
        {
            "phase": phase,
            "archive_bytes": archive_bytes,
            "grouped_bytes": grouped_bytes,
            "row_count": row_count,
            "group_count": group_count,
        },
    )


def clear_memory_metrics(exchange: str) -> None:
    """清理期货成交下载的内存观测。"""
    cex_config.clear_runtime_memory_metrics(DATASET_ID, exchange)


def estimate_rows_by_date_memory(rows_by_date: dict) -> tuple[int, int, int]:
    """估算按日期分组记录的内存占用。"""
    total_bytes = sys.getsizeof(rows_by_date)
    total_rows = 0
    total_groups = len(rows_by_date)
    for date_str, rows in rows_by_date.items():
        total_bytes += sys.getsizeof(date_str)
        total_bytes += sys.getsizeof(rows)
        total_rows += len(rows)
        for row in rows:
            total_bytes += sys.getsizeof(row)
    return total_bytes, total_rows, total_groups


def estimate_rows_by_symbol_memory(rows_by_symbol: dict) -> tuple[int, int, int]:
    """估算按交易对与日期分组记录的内存占用。"""
    total_bytes = sys.getsizeof(rows_by_symbol)
    total_rows = 0
    total_groups = 0
    for symbol, rows_by_date in rows_by_symbol.items():
        total_bytes += sys.getsizeof(symbol)
        total_bytes += sys.getsizeof(rows_by_date)
        grouped_bytes, grouped_rows, grouped_count = estimate_rows_by_date_memory(rows_by_date)
        total_bytes += grouped_bytes
        total_rows += grouped_rows
        total_groups += grouped_count
    return total_bytes, total_rows, total_groups


def build_fail_log_path() -> Path:
    """构造失败日志路径。"""
    return FAIL_LOG_DIR / "download_failures.json"


def build_output_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造输出文件路径。"""
    return base_dir / symbol / f"{symbol}{date_str}.csv.gz"


def build_okx_delivery_state_path(base_dir: Path, family: str, date_str: str) -> Path:
    """构造OKX交割合约家族同步标记路径。"""
    return base_dir / "__okx_futureschain_state" / family / f"{date_str}.done"


def build_bitget_output_path(base_dir: Path, symbol: str, shard_name: str) -> Path:
    """构造Bitget原始分片路径。"""
    return base_dir / symbol / shard_name


def append_failure(fail_path: Path, failure_key: str, exchange: str, symbol: str, target: str, message: str) -> None:
    """记录失败项。"""
    update_failure_file(
        fail_path,
        {
            "键": failure_key,
            "交易所": exchange,
            "交易对": symbol,
            "目标": target,
            "错误信息": message,
            "记录时间": datetime.now(tz=timezone.utc).isoformat(),
        },
        failure_key,
    )


def clear_failure(fail_path: Path, failure_key: str) -> None:
    """清理失败项。"""
    update_failure_file(fail_path, None, failure_key)


def request_okx_json(url: str) -> dict:
    """请求OKX公共JSON接口。"""
    request = Request(
        url,
        headers={
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Accept": HTTP_HEADER_ACCEPT_JSON,
            "Accept-Language": HTTP_HEADER_ACCEPT_LANGUAGE,
            "Referer": HTTP_HEADER_OKX_REFERER,
            "Origin": HTTP_HEADER_OKX_ORIGIN,
        },
    )
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise NetworkRequestError(f"OKX接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise NetworkRequestError("OKX接口请求失败: 网络错误") from exc
    except TimeoutError as exc:
        raise NetworkRequestError("OKX接口请求失败: 超时") from exc
    except socket.timeout as exc:
        raise NetworkRequestError("OKX接口请求失败: 超时") from exc


def build_okx_query_ms(day_text: str) -> int:
    """构造OKX查询起始毫秒时间戳。"""
    dt = datetime.strptime(day_text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000) - OKX_MODULE_1_DATE_TZ_OFFSET_HOURS * 3600 * 1000


def build_okx_query_range(start_day: str, end_day: str) -> tuple[int, int]:
    """构造OKX查询时间范围。"""
    begin_ms = build_okx_query_ms(start_day)
    next_day = (datetime.strptime(end_day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    end_ms = build_okx_query_ms(next_day) - 1
    return begin_ms, end_ms


def build_bitget_request(url: str) -> Request:
    """构造Bitget下载请求。"""
    return Request(
        url,
        headers={
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Accept": HTTP_HEADER_ACCEPT_ALL,
            "Accept-Language": HTTP_HEADER_ACCEPT_LANGUAGE,
            "Referer": HTTP_HEADER_BITGET_REFERER,
            "Origin": HTTP_HEADER_BITGET_ORIGIN,
        },
    )


def resolve_bybit_symbols() -> tuple[list[str], dict[str, str]]:
    """解析Bybit期货成交交易对列表与起始日期。"""
    symbols = set(cex_config.get_future_trade_symbols("bybit"))
    start_dates = {}
    try:
        symbols.update(list_bybit_delivery_symbols_since(cex_config.get_min_start_date(DATASET_ID, "bybit")))
        start_dates.update(list_bybit_delivery_symbol_start_dates(cex_config.get_min_start_date(DATASET_ID, "bybit")))
    except NetworkRequestError as exc:
        log(f"bybit future 动态交割合约刷新失败，继续使用静态列表: {exc}")
    return sorted(symbols), start_dates


def resolve_okx_symbols() -> tuple[list[str], dict[str, str]]:
    """解析OKX期货成交交易对列表与起始日期。"""
    symbols = set(cex_config.get_future_trade_symbols("okx"))
    symbols.update(cex_config.get_delivery_families("okx"))
    return sorted(symbols), {}


def parse_bitget_date_from_name(file_name: str) -> str | None:
    """从Bitget文件名解析日期。"""
    if file_name.endswith(".csv.gz"):
        date_str = file_name.removesuffix(".csv.gz")[-10:]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            return date_str
    matched = BITGET_ARCHIVE_NAME_PATTERN.match(file_name)
    if not matched:
        return None
    raw_date = matched.group(1)
    return f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"


def get_existing_dates(base_dir: Path, exchange: str, symbol: str) -> set[str]:
    """获取存储中已经存在的日期集合。"""
    dates = set()
    for file_name in list_storage_file_names(base_dir / symbol):
        if exchange == "bitget":
            date_str = parse_bitget_date_from_name(file_name)
        else:
            if not file_name.endswith(".csv.gz"):
                continue
            date_str = file_name.removesuffix(".csv.gz")[-10:]
        if date_str and re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            dates.add(date_str)
    return dates


def get_existing_okx_delivery_family_dates(base_dir: Path, family: str) -> set[str]:
    """获取OKX交割合约家族已完成日期集合。"""
    dates = set()
    for file_name in list_storage_file_names(base_dir / "__okx_futureschain_state" / family):
        if not file_name.endswith(".done"):
            continue
        date_text = file_name.removesuffix(".done")
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
            dates.add(date_text)
    return dates


def write_okx_delivery_family_state(base_dir: Path, family: str, date_str: str) -> None:
    """写入OKX交割合约家族同步标记。"""
    state_path = build_okx_delivery_state_path(base_dir, family, date_str)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_name(state_path.name + ".part")
    tmp_path.write_text(date_str, encoding="utf-8")
    replace_output_file(tmp_path, state_path)


def get_latest_local_date(base_dir: Path, symbol: str) -> str | None:
    """获取本地最新日期。"""
    symbol_dir = base_dir / symbol
    if not symbol_dir.exists():
        return None
    dates = []
    for path in symbol_dir.glob(f"{symbol}*.csv.gz"):
        date_str = path.name.removesuffix(".csv.gz")[-10:]
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            continue
        dates.append(date_str)
    return max(dates) if dates else None


def get_latest_bitget_local_date(base_dir: Path, symbol: str) -> str | None:
    """获取Bitget本地最新日期。"""
    symbol_dir = base_dir / symbol
    if not symbol_dir.exists():
        return None
    dates = []
    for path in symbol_dir.glob(f"{symbol}*.csv.gz"):
        date_str = path.name.removesuffix(".csv.gz")[-10:]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            dates.append(date_str)
    for path in symbol_dir.glob("*.zip"):
        matched = BITGET_ARCHIVE_NAME_PATTERN.match(path.name)
        if not matched:
            continue
        raw_date = matched.group(1)
        date_str = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        dates.append(date_str)
    return max(dates) if dates else None


def count_local_synced_days(base_dir: Path, exchange: str, symbol: str) -> int:
    """统计本地已同步天数。"""
    symbol_dir = base_dir / symbol
    if not symbol_dir.exists():
        return 0
    if exchange == "bitget":
        dates = set()
        for path in symbol_dir.glob(f"{symbol}*.csv.gz"):
            date_str = path.name.removesuffix(".csv.gz")[-10:]
            if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
                dates.add(date_str)
        for path in symbol_dir.glob("*.zip"):
            matched = BITGET_ARCHIVE_NAME_PATTERN.match(path.name)
            if not matched:
                continue
            raw_date = matched.group(1)
            dates.add(f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}")
        return len(dates)
    dates = set()
    for path in symbol_dir.glob(f"{symbol}*.csv.gz"):
        date_str = path.name.removesuffix(".csv.gz")[-10:]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            dates.add(date_str)
    return len(dates)


def next_sync_date(start_date: str, latest_local: str | None) -> str:
    """计算下一次需要同步的日期。"""
    if not latest_local:
        return start_date
    return (datetime.strptime(latest_local, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")


def count_days_in_range(start_day: str, end_day: str) -> int:
    """统计日期区间内的自然日数量。"""
    start_dt = datetime.strptime(start_day, "%Y-%m-%d")
    end_dt = datetime.strptime(end_day, "%Y-%m-%d")
    return (end_dt - start_dt).days + 1


def download_bybit_day(base_dir: Path, symbol: str, date_str: str, fail_path: Path) -> bool:
    """下载Bybit单日期货成交文件。"""
    output_path = build_output_path(base_dir, symbol, date_str)
    if output_path.exists():
        clear_failure(fail_path, f"bybit:{symbol}:{date_str}")
        return True
    url = f"{BYBIT_BASE_URL}/{symbol}/{symbol}{date_str}.csv.gz"
    try:
        content = download_bytes(url, TIMEOUT_SECONDS)
    except RuntimeError as exc:
        append_failure(fail_path, f"bybit:{symbol}:{date_str}", "bybit", symbol, date_str, str(exc))
        return False
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_name(output_path.name + ".part")
    tmp_path.write_bytes(content)
    if not is_valid_gzip_file(tmp_path):
        tmp_path.unlink()
        append_failure(fail_path, f"bybit:{symbol}:{date_str}", "bybit", symbol, date_str, "下载文件损坏")
        return False
    replace_output_file(tmp_path, output_path)
    clear_failure(fail_path, f"bybit:{symbol}:{date_str}")
    return True


def write_daily_rows(base_dir: Path, symbol: str, rows_by_date: dict) -> int:
    """按日写入标准化成交文件。"""
    total = 0
    for date_str, rows in rows_by_date.items():
        output_path = build_output_path(base_dir, symbol, date_str)
        if output_path.exists():
            continue
        total += write_gzip_csv_rows(output_path, FUTURE_TRADE_FIELDS, rows, False)
    return total


def write_daily_rows_by_symbol(base_dir: Path, rows_by_symbol: dict) -> int:
    """按交易对和日期写入标准化成交文件。"""
    total = 0
    for symbol, rows_by_date in rows_by_symbol.items():
        total += write_daily_rows(base_dir, symbol, rows_by_date)
    return total


def append_rows_to_file(file_path: Path, rows: list[dict]) -> int:
    """向压缩成交文件追加标准化记录。"""
    return write_gzip_csv_rows(file_path, FUTURE_TRADE_FIELDS, rows, True)


def append_rows_to_temp_file(file_path: Path, rows: list[dict]) -> int:
    """向临时压缩成交文件追加标准化记录。"""
    if not rows:
        return 0
    file_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not file_path.exists()
    mode = "at" if file_path.exists() else "wt"
    with gzip.open(file_path, mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FUTURE_TRADE_FIELDS)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return len(rows)


def split_binance_zip(content: bytes, symbol: str, base_dir: Path) -> int:
    """拆分Binance压缩期货成交文件。"""
    rows_by_date = defaultdict(list)
    with zipfile.ZipFile(BytesIO(content), "r") as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            reader = csv.reader(TextIOWrapper(f, encoding="utf-8"))
            for parts in reader:
                normalized = normalize_binance_future_parts(symbol, parts)
                if normalized:
                    date_str, row = normalized
                    rows_by_date[date_str].append(row)
    grouped_bytes, total_rows, total_groups = estimate_rows_by_date_memory(rows_by_date)
    update_memory_metrics("binance", "月包拆分", len(content), grouped_bytes, total_rows, total_groups)
    return write_daily_rows(base_dir, symbol, rows_by_date)


def split_okx_zip(content: bytes, base_dir: Path) -> int:
    """拆分OKX压缩期货成交文件。"""
    rows_by_symbol = defaultdict(lambda: defaultdict(list))
    with zipfile.ZipFile(BytesIO(content), "r") as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8"))
            for row in reader:
                normalized = normalize_okx_future_trade_row(row)
                if normalized:
                    date_str, item = normalized
                    rows_by_symbol[item["symbol"]][date_str].append(item)
    grouped_bytes, total_rows, total_groups = estimate_rows_by_symbol_memory(rows_by_symbol)
    update_memory_metrics("okx", "月包拆分", len(content), grouped_bytes, total_rows, total_groups)
    return write_daily_rows_by_symbol(base_dir, rows_by_symbol)


def download_binance_archive(base_dir: Path, symbol: str, url: str, failure_key: str, fail_path: Path) -> bool:
    """下载并拆分Binance期货成交归档。"""
    try:
        update_memory_metrics("binance", "下载月包", 0, 0, 0, 0)
        content = download_bytes(url, TIMEOUT_SECONDS)
    except RuntimeError as exc:
        append_failure(fail_path, failure_key, "binance", symbol, url, str(exc))
        return False
    update_memory_metrics("binance", "下载完成", len(content), 0, 0, 0)
    count = split_binance_zip(content, symbol, base_dir)
    update_memory_metrics("binance", "写入完成", 0, 0, 0, 0)
    clear_failure(fail_path, failure_key)
    log(f"binance {symbol} 已写入记录数: {count}")
    return True


def download_okx_archive(base_dir: Path, symbol: str, url: str, failure_key: str, fail_path: Path) -> bool:
    """下载并拆分OKX期货成交归档。"""
    try:
        update_memory_metrics("okx", "下载月包", 0, 0, 0, 0)
        content = download_bytes(url, TIMEOUT_SECONDS)
    except RuntimeError as exc:
        append_failure(fail_path, failure_key, "okx", symbol, url, str(exc))
        return False
    update_memory_metrics("okx", "下载完成", len(content), 0, 0, 0)
    count = split_okx_zip(content, base_dir)
    update_memory_metrics("okx", "写入完成", 0, 0, 0, 0)
    clear_failure(fail_path, failure_key)
    log(f"okx {symbol} 已写入记录数: {count}")
    return True


def download_bitget_bytes(url: str) -> bytes | None:
    """下载Bitget期货成交分片。"""
    request = build_bitget_request(url)
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return response.read()
    except HTTPError as exc:
        if exc.code in {403, 404}:
            return None
        raise RuntimeError(f"下载失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("下载失败: 网络错误") from exc
    except TimeoutError as exc:
        raise RuntimeError("下载失败: 超时") from exc
    except socket.timeout as exc:
        raise RuntimeError("下载失败: 超时") from exc


def download_bitget_day(base_dir: Path, symbol: str, date_str: str, fail_path: Path) -> bool:
    """下载Bitget单日期货成交分片。"""
    failure_key = f"bitget:{symbol}:{date_str}"
    output_path = build_output_path(base_dir, symbol, date_str)
    if output_path.exists():
        clear_failure(fail_path, failure_key)
        return True
    tmp_path = output_path.with_name(output_path.name + ".part")
    if tmp_path.exists():
        tmp_path.unlink()
    shard_count = 0
    row_count = 0
    for index in range(1, BITGET_MAX_FILE_INDEX + 1):
        shard_tag = f"{index:03d}"
        shard_name = f"{date_str.replace('-', '')}_{shard_tag}.zip"
        url = f"{BITGET_FUTURE_BASE_URL}/{symbol}/{shard_name}"
        try:
            content = download_bitget_bytes(url)
        except RuntimeError as exc:
            append_failure(fail_path, failure_key, "bitget", symbol, url, str(exc))
            return False
        if content is None:
            if shard_count == 0:
                clear_failure(fail_path, failure_key)
                return False
            if row_count == 0:
                clear_failure(fail_path, failure_key)
                return False
            if not is_valid_gzip_file(tmp_path):
                tmp_path.unlink()
                append_failure(fail_path, failure_key, "bitget", symbol, date_str, "生成文件损坏")
                return False
            replace_output_file(tmp_path, output_path)
            clear_failure(fail_path, failure_key)
            log(f"bitget {symbol} {date_str} 已写入记录数: {row_count}")
            return True
        rows = []
        with zipfile.ZipFile(BytesIO(content), "r") as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                reader = csv.DictReader(TextIOWrapper(f, encoding="utf-8", newline=""))
                for row in reader:
                    normalized = normalize_bitget_future_trade_row(symbol, row)
                    if normalized:
                        rows.append(normalized[1])
        row_count += append_rows_to_temp_file(tmp_path, rows)
        shard_count += 1
        time.sleep(REQUEST_MIN_INTERVAL_SECONDS)
    if row_count > 0:
        if not is_valid_gzip_file(tmp_path):
            tmp_path.unlink()
            append_failure(fail_path, failure_key, "bitget", symbol, date_str, "生成文件损坏")
            return False
        replace_output_file(tmp_path, output_path)
        log(f"bitget {symbol} {date_str} 已写入记录数: {row_count}")
        clear_failure(fail_path, failure_key)
        return True
    clear_failure(fail_path, failure_key)
    return False


def sync_binance_month(symbol: str, missing_dates: list[str], fail_path: Path, synced_days: int) -> tuple[int, bool]:
    """同步Binance单月期货成交。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, "binance")
    if not base_dir:
        return synced_days, True
    if not missing_dates:
        return synced_days, True
    start_day = missing_dates[0]
    end_day = missing_dates[-1]
    month_tag = start_day[:7]
    monthly_file = f"{symbol}-trades-{month_tag}.zip"
    monthly_url = f"{BINANCE_BUCKET_URL}/data/futures/um/monthly/trades/{symbol}/{monthly_file}"
    month_failure_key = f"binance:{symbol}:month:{month_tag}"
    status_update("binance", symbol, (synced_days, f"月 {month_tag} 请求中"))
    success = download_binance_archive(base_dir, symbol, monthly_url, month_failure_key, fail_path)
    if success:
        synced_days += len(missing_dates)
        status_update("binance", symbol, (synced_days, f"月 {month_tag} {monthly_file}"))
        return synced_days, True
    for date_str in missing_dates:
        if cex_config.apply_pause_if_requested(DATASET_ID, "binance"):
            status_update("binance", symbol, cex_config.PAUSED_STATUS_TEXT)
            return synced_days, False
        daily_file = f"{symbol}-trades-{date_str}.zip"
        daily_url = f"{BINANCE_BUCKET_URL}/data/futures/um/daily/trades/{symbol}/{daily_file}"
        status_update("binance", symbol, (synced_days, f"日 {date_str} 请求中"))
        if download_binance_archive(base_dir, symbol, daily_url, f"binance:{symbol}:day:{date_str}", fail_path):
            synced_days += 1
            status_update("binance", symbol, (synced_days, f"日 {date_str} {daily_file}"))
        else:
            status_update("binance", symbol, (synced_days, f"失败 {date_str}"))
    return synced_days, True


def sync_bitget_month(symbol: str, missing_dates: list[str], fail_path: Path, synced_days: int) -> tuple[int, bool]:
    """同步Bitget单月期货成交。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, "bitget")
    if not base_dir:
        return synced_days, True
    for date_str in missing_dates:
        if cex_config.apply_pause_if_requested(DATASET_ID, "bitget"):
            status_update("bitget", symbol, cex_config.PAUSED_STATUS_TEXT)
            return synced_days, False
        status_update("bitget", symbol, (synced_days, f"日 {date_str} 请求中"))
        log(f"bitget {symbol} 请求日包: {date_str}")
        if download_bitget_day(base_dir, symbol, date_str, fail_path):
            synced_days += 1
            status_update("bitget", symbol, (synced_days, f"日 {date_str} {symbol}{date_str}.csv.gz"))
        else:
            status_update("bitget", symbol, (synced_days, f"无文件 {date_str}"))
    return synced_days, True


def fetch_okx_download_urls(symbol: str, start_day: str, end_day: str, date_aggr_type: str) -> list:
    """获取OKX期货成交归档链接。"""
    delivery_families = set(cex_config.get_delivery_families("okx"))
    inst_type = "SWAP" if symbol.endswith("-SWAP") else "FUTURES"
    begin_ms, end_ms = build_okx_query_range(start_day, end_day)
    params = {
        "module": "1",
        "instType": inst_type,
        "dateAggrType": date_aggr_type,
        "begin": str(begin_ms),
        "end": str(end_ms),
    }
    if inst_type == "SWAP":
        parts = symbol.split("-")
        params["instFamilyList"] = f"{parts[0]}-{parts[1]}"
    elif symbol in delivery_families:
        params["instFamilyList"] = symbol
    else:
        params["instIdList"] = symbol
    data = request_okx_json(f"{OKX_MARKET_HISTORY_URL}?{urlencode(params)}")
    if data.get("code") != "0":
        raise RuntimeError(f"接口返回错误: {data.get('msg')}")
    urls = []
    for data_item in data.get("data", []):
        for detail in data_item.get("details", []):
            for item in detail.get("groupDetails", []):
                url = item.get("url", "")
                file_name = item.get("filename", "")
                if url and (file_name.startswith(symbol) or (symbol in delivery_families and file_name.startswith(f"{symbol}-futureschain-"))):
                    urls.append(url)
    return urls


def sync_okx_month(symbol: str, missing_dates: list[str], fail_path: Path, synced_days: int) -> tuple[int, bool]:
    """同步OKX单月期货成交。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, "okx")
    if not base_dir:
        return synced_days, True
    if not missing_dates:
        return synced_days, True
    start_day = missing_dates[0]
    end_day = missing_dates[-1]
    month_tag = start_day[:7]
    status_update("okx", symbol, (synced_days, f"月 {month_tag} 请求中"))
    log(f"okx {symbol} 请求月包: {month_tag}")
    try:
        urls = fetch_okx_download_urls(symbol, start_day, end_day, "monthly")
    except NetworkRequestError as exc:
        log(f"okx {symbol} 月包请求失败，改走日包: {exc}")
        urls = []
    time.sleep(REQUEST_MIN_INTERVAL_SECONDS)
    if urls:
        for url in urls:
            file_name = url.rsplit("/", 1)[-1]
            if download_okx_archive(base_dir, symbol, url, f"okx:{symbol}:month:{month_tag}", fail_path):
                if symbol in cex_config.get_delivery_families("okx"):
                    for day_text in missing_dates:
                        write_okx_delivery_family_state(base_dir, symbol, day_text)
                synced_days += len(missing_dates)
                status_update("okx", symbol, (synced_days, f"月 {month_tag} {file_name}"))
                return synced_days, True
    for day_text in missing_dates:
        if cex_config.apply_pause_if_requested(DATASET_ID, "okx"):
            status_update("okx", symbol, cex_config.PAUSED_STATUS_TEXT)
            return synced_days, False
        status_update("okx", symbol, (synced_days, f"日 {day_text} 请求中"))
        log(f"okx {symbol} 请求日包: {day_text}")
        try:
            urls = fetch_okx_download_urls(symbol, day_text, day_text, "daily")
        except NetworkRequestError as exc:
            log(f"okx {symbol} {day_text} 日包请求失败，跳过当日: {exc}")
            status_update("okx", symbol, (synced_days, f"失败 {day_text}"))
            continue
        time.sleep(REQUEST_MIN_INTERVAL_SECONDS)
        if not urls:
            status_update("okx", symbol, (synced_days, f"无文件 {day_text}"))
            continue
        file_name = urls[0].rsplit("/", 1)[-1]
        if download_okx_archive(base_dir, symbol, urls[0], f"okx:{symbol}:day:{day_text}", fail_path):
            if symbol in cex_config.get_delivery_families("okx"):
                write_okx_delivery_family_state(base_dir, symbol, day_text)
            synced_days += 1
            status_update("okx", symbol, (synced_days, f"日 {day_text} {file_name}"))
        else:
            status_update("okx", symbol, (synced_days, f"失败 {day_text}"))
    return synced_days, True


def sync_bybit_month(symbol: str, missing_dates: list[str], fail_path: Path, synced_days: int) -> tuple[int, bool]:
    """同步Bybit单月期货成交。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, "bybit")
    if not base_dir:
        return synced_days, True
    for date_str in missing_dates:
        if cex_config.apply_pause_if_requested(DATASET_ID, "bybit"):
            status_update("bybit", symbol, cex_config.PAUSED_STATUS_TEXT)
            return synced_days, False
        status_update("bybit", symbol, (synced_days, f"日 {date_str} 请求中"))
        if download_bybit_day(base_dir, symbol, date_str, fail_path):
            synced_days += 1
            status_update("bybit", symbol, (synced_days, f"日 {date_str} {symbol}{date_str}.csv.gz"))
        else:
            status_update("bybit", symbol, (synced_days, f"失败 {date_str}"))
    return synced_days, True


def run_exchange(exchange: str, symbols: list, month_worker, dynamic_start_dates: dict[str, str] | None = None) -> None:
    """按月执行单个交易所同步。"""
    if not cex_config.is_supported(DATASET_ID, exchange):
        clear_memory_metrics(exchange)
        for symbol in symbols:
            status_update(exchange, symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    if cex_config.apply_pause_if_requested(DATASET_ID, exchange):
        clear_memory_metrics(exchange)
        for symbol in symbols:
            status_update(exchange, symbol, cex_config.PAUSED_STATUS_TEXT)
        return
    fail_path = build_fail_log_path()
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    if not base_dir:
        clear_memory_metrics(exchange)
        return
    end_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    if end_dt < datetime(2000, 1, 1):
        return
    dynamic_start_dates = dynamic_start_dates or {}
    state_list = []
    month_anchor_dates = []
    end_date = end_dt.strftime("%Y-%m-%d")
    for symbol in symbols:
        if cex_config.apply_pause_if_requested(DATASET_ID, exchange):
            status_update(exchange, symbol, cex_config.PAUSED_STATUS_TEXT)
            return
        start_date = dynamic_start_dates.get(symbol) or cex_config.get_start_date(DATASET_ID, exchange, symbol) or cex_config.get_min_start_date(DATASET_ID, exchange)
        if not start_date:
            status_update(exchange, symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
            continue
        if exchange == "okx" and symbol in cex_config.get_delivery_families("okx"):
            existing_dates = get_existing_okx_delivery_family_dates(base_dir, symbol)
        else:
            existing_dates = get_existing_dates(base_dir, exchange, symbol)
        synced_days = count_existing_days(existing_dates, start_date, end_date)
        synced_until = get_synced_until_date(existing_dates, start_date, end_date)
        missing_dates = list_missing_dates(existing_dates, start_date, end_date)
        if not missing_dates:
            latest_text = synced_until or end_date
            status_update(exchange, symbol, (synced_days, f"日 {latest_text} 已是最新"))
            continue
        next_date = missing_dates[0]
        if synced_until:
            status_update(exchange, symbol, (synced_days, f"日 {synced_until} 准备回补"))
        else:
            status_update(exchange, symbol, (synced_days, f"准备 {next_date}"))
        log(f"{exchange} {symbol} 开始回补: {next_date} -> {end_date}")
        state_list.append({"symbol": symbol, "missing_dates": set(missing_dates), "synced_days": synced_days})
        month_anchor_dates.append(next_date)
    if not state_list:
        clear_memory_metrics(exchange)
        return
    for month_start_day in iter_months(min(month_anchor_dates), end_date):
        if cex_config.apply_pause_if_requested(DATASET_ID, exchange):
            for state in state_list:
                status_update(exchange, state["symbol"], cex_config.PAUSED_STATUS_TEXT)
            return
        month_finish_day = min(end_date, month_end(month_start_day))
        for state in state_list:
            if cex_config.apply_pause_if_requested(DATASET_ID, exchange):
                status_update(exchange, state["symbol"], cex_config.PAUSED_STATUS_TEXT)
                return
            month_missing_dates = [
                date_text
                for date_text in iter_dates(month_start_day, month_finish_day)
                if date_text in state["missing_dates"]
            ]
            if not month_missing_dates:
                continue
            state["synced_days"], is_done = month_worker(
                state["symbol"],
                month_missing_dates,
                fail_path,
                state["synced_days"],
            )
            if not is_done:
                return
            for date_text in month_missing_dates:
                state["missing_dates"].discard(date_text)
    clear_memory_metrics(exchange)


def main() -> None:
    """运行期货成交下载主循环。"""
    while True:
        bybit_symbols, bybit_start_dates = resolve_bybit_symbols()
        okx_symbols, okx_start_dates = resolve_okx_symbols()
        threads = [
            threading.Thread(
                target=run_exchange,
                args=("bybit", bybit_symbols, sync_bybit_month, bybit_start_dates),
                daemon=True,
            ),
            threading.Thread(
                target=run_exchange,
                args=("binance", cex_config.get_future_trade_symbols("binance"), sync_binance_month),
                daemon=True,
            ),
            threading.Thread(
                target=run_exchange,
                args=("bitget", cex_config.get_future_trade_symbols("bitget"), sync_bitget_month),
                daemon=True,
            ),
            threading.Thread(
                target=run_exchange,
                args=("okx", okx_symbols, sync_okx_month, okx_start_dates),
                daemon=True,
            ),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        sleep_seconds = seconds_until_next_utc_4h()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 4小时倍数）")
        cex_config.wait_with_task_control(sleep_seconds)


def run() -> None:
    """兼容启动器运行入口。"""
    main()


if __name__ == "__main__":
    run()
