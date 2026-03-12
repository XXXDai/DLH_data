from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import socket
import tarfile
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import app_config
from cex import cex_config
from cex.cex_common import iter_dates
from cex.cex_common import load_failures
from cex.cex_common import seconds_until_next_utc_4h
from cex.cex_common import update_failure_file
from cex.cex_common import build_part_path
from cex.cex_common import cleanup_stale_part_file
from cex.cex_common import replace_output_file
from cex.cex_orderbook_ws_common import NetworkRequestError
from cex.cex_orderbook_ws_common import list_bybit_delivery_symbols_since


BYBIT_BASE_URLS = {
    "future": "https://quote-saver.bycsi.com/orderbook/linear",  # Bybit期货订单簿归档根地址，字符串
    "spot": "https://quote-saver.bycsi.com/orderbook/spot",  # Bybit现货订单簿归档根地址，字符串
}  # Bybit订单簿归档根地址映射，映射
BINANCE_BASE_URLS = {
    "future": "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/data/futures/um/daily/bookTicker",  # Binance期货BBO归档根地址，字符串
}  # Binance订单簿归档根地址映射，映射
BITGET_BASE_URLS = {
    "future": "https://img.bitgetimg.com/online/depth",  # Bitget期货BBO归档根地址，字符串
    "spot": "https://img.bitgetimg.com/online/depth",  # Bitget现货BBO归档根地址，字符串
}  # Bitget订单簿归档根地址映射，映射
BITGET_MARKET_PATHS = {
    "future": "2",  # Bitget期货订单簿市场路径，字符串
    "spot": "1",  # Bitget现货订单簿市场路径，字符串
}  # Bitget订单簿市场路径映射，映射
OKX_BASE_URLS = {
    "future": "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily",  # OKX期货订单簿归档根地址，字符串
    "spot": "https://static.okx.com/cdn/okx/match/orderbook/L2/400lv/daily",  # OKX现货订单簿归档根地址，字符串
}  # OKX订单簿归档根地址映射，映射
OKX_ORDERBOOK_LEVELS = {
    "future": "400lv",  # OKX期货订单簿档位标签，字符串
    "spot": "400lv",  # OKX现货订单簿档位标签，字符串
}  # OKX订单簿档位标签映射，映射
HTTP_HEADER_USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"  # 请求头浏览器标识，字符串
HTTP_HEADER_ACCEPT_ALL = "*/*"  # 文件请求头可接受类型，字符串
HTTP_HEADER_ACCEPT_LANGUAGE = "en-US,en;q=0.9"  # 请求头语言偏好，字符串
HTTP_HEADER_BITGET_REFERER = "https://www.bitget.com/"  # Bitget请求头来源地址，字符串
HTTP_HEADER_BITGET_ORIGIN = "https://www.bitget.com"  # Bitget请求头来源域名，字符串
HTTP_HEADER_OKX_REFERER = "https://www.okx.com/"  # OKX请求头来源地址，字符串
HTTP_HEADER_OKX_ORIGIN = "https://www.okx.com"  # OKX请求头来源域名，字符串
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")  # 日期格式正则，正则
DATASET_IDS = {
    "future": "D10001",  # 期货订单簿数据集标识，字符串
    "spot": "D10005",  # 现货订单簿数据集标识，字符串
}  # 市场到数据集映射，映射
TIMEOUT_SECONDS = app_config.DOWNLOAD_TIMEOUT_SECONDS  # 下载超时，秒
RETRY_TIMES = app_config.RETRY_TIMES  # 下载重试次数，次
RETRY_INTERVAL_SECONDS = app_config.RETRY_INTERVAL_SECONDS  # 下载重试间隔，秒
CHUNK_SIZE = app_config.CHUNK_SIZE  # 下载块大小，字节
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
    """更新统一状态键的状态值。"""
    if STATUS_HOOK:
        STATUS_HOOK(cex_config.get_status_key(exchange, market, symbol), value)


def dataset_id_for_market(market: str) -> str:
    """返回市场对应的数据集标识。"""
    return DATASET_IDS[market]


def is_valid_ymd(date_text: str) -> bool:
    """判断日期文本是否为年月日格式。"""
    return bool(DATE_PATTERN.fullmatch(date_text))


def data_dir_for_market(market: str, exchange: str) -> Path | None:
    """返回市场与交易所对应的输出目录。"""
    return cex_config.get_source_dir(dataset_id_for_market(market), exchange)


def fail_log_path_for_market(market: str) -> Path:
    """返回市场对应的失败日志路径。"""
    return Path(dataset_id_for_market(market)) / "download_failures.json"


def build_bybit_url(market: str, symbol: str, date_str: str) -> str:
    """构造Bybit订单簿归档地址。"""
    return f"{BYBIT_BASE_URLS[market]}/{symbol}/{date_str}_{symbol}_ob200.data.zip"


def build_binance_url(market: str, symbol: str, date_str: str) -> str:
    """构造Binance订单簿归档地址。"""
    return f"{BINANCE_BASE_URLS[market]}/{symbol}/{symbol}-bookTicker-{date_str}.zip"


def build_bitget_url(market: str, symbol: str, date_str: str) -> str:
    """构造Bitget订单簿归档地址。"""
    return f"{BITGET_BASE_URLS[market]}/{symbol}/{BITGET_MARKET_PATHS[market]}/{date_str.replace('-', '')}.zip"


def okx_orderbook_level_for_market(market: str) -> str:
    """返回OKX市场对应的订单簿档位标签。"""
    return OKX_ORDERBOOK_LEVELS[market]


def build_okx_url(market: str, symbol: str, date_str: str) -> str:
    """构造OKX订单簿归档地址。"""
    date_tag = date_str.replace("-", "")
    file_name = build_okx_file_name(market, symbol, date_str)
    return f"{OKX_BASE_URLS[market]}/{date_tag}/{file_name}"


def build_okx_file_name(market: str, symbol: str, date_str: str) -> str:
    """构造OKX订单簿文件名。"""
    return f"{symbol}-L2orderbook-{okx_orderbook_level_for_market(market)}-{date_str}.tar.gz"


def build_url(exchange: str, market: str, symbol: str, date_str: str) -> str:
    """构造指定交易所订单簿归档地址。"""
    if exchange == "bybit":
        return build_bybit_url(market, symbol, date_str)
    if exchange == "binance":
        return build_binance_url(market, symbol, date_str)
    if exchange == "bitget":
        return build_bitget_url(market, symbol, date_str)
    return build_okx_url(market, symbol, date_str)


def build_output_path(exchange: str, market: str, base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造归档输出路径。"""
    if exchange == "bybit":
        return base_dir / symbol / f"{date_str}_{symbol}_ob200.data.zip"
    if exchange == "binance":
        return base_dir / symbol / f"{symbol}-bookTicker-{date_str}.zip"
    if exchange == "bitget":
        return base_dir / symbol / f"{date_str.replace('-', '')}.zip"
    return base_dir / symbol / build_okx_file_name(market, symbol, date_str)


def parse_date_from_name(exchange: str, market: str, file_name: str, symbol: str) -> str | None:
    """从文件名中解析日期。"""
    if exchange == "bybit":
        suffix = f"_{symbol}_ob200.data.zip"
        if not file_name.endswith(suffix):
            return None
        date_text = file_name[: -len(suffix)]
        return date_text if is_valid_ymd(date_text) else None
    if exchange == "binance":
        prefix = f"{symbol}-bookTicker-"
        suffix = ".zip"
        if not file_name.startswith(prefix) or not file_name.endswith(suffix):
            return None
        date_text = file_name[len(prefix) : -len(suffix)]
        return date_text if is_valid_ymd(date_text) else None
    if exchange == "bitget":
        if not file_name.endswith(".zip"):
            return None
        raw_date = file_name.removesuffix(".zip")
        if len(raw_date) != 8 or not raw_date.isdigit():
            return None
        date_text = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        return date_text if is_valid_ymd(date_text) else None
    prefix = f"{symbol}-L2orderbook-{okx_orderbook_level_for_market(market)}-"
    suffix = ".tar.gz"
    if not file_name.startswith(prefix) or not file_name.endswith(suffix):
        return None
    date_text = file_name[len(prefix) : -len(suffix)]
    return date_text if is_valid_ymd(date_text) else None


def iter_symbol_paths(exchange: str, market: str, symbol_dir: Path):
    """遍历交易对目录中的归档文件。"""
    if exchange == "bybit":
        yield from symbol_dir.glob("*_ob200.data.zip")
        return
    if exchange == "binance":
        yield from symbol_dir.glob("*-bookTicker-*.zip")
        return
    if exchange == "bitget":
        yield from symbol_dir.glob("*.zip")
        return
    yield from symbol_dir.glob(f"*-L2orderbook-{okx_orderbook_level_for_market(market)}-*.tar.gz")


def get_latest_local_date(base_dir: Path, exchange: str, market: str, symbol: str) -> str | None:
    """获取本地已下载的最新日期。"""
    symbol_dir = base_dir / symbol
    if not symbol_dir.exists():
        return None
    dates = []
    for file_path in iter_symbol_paths(exchange, market, symbol_dir):
        date_text = parse_date_from_name(exchange, market, file_path.name, symbol)
        if date_text:
            dates.append(date_text)
    return max(dates) if dates else None


def record_failure(exchange: str, market: str, symbol: str, date_str: str, message: str) -> None:
    """记录单个日期的失败信息。"""
    update_failure_file(
        fail_log_path_for_market(market),
        {
            "键": f"{exchange}:{market}:{symbol}:{date_str}",
            "交易所": exchange,
            "市场": market,
            "交易对": symbol,
            "目标": date_str,
            "错误信息": message,
            "记录时间": datetime.now(tz=timezone.utc).isoformat(),
        },
        f"{exchange}:{market}:{symbol}:{date_str}",
    )


def clear_failure(exchange: str, market: str, symbol: str, date_str: str) -> None:
    """清理单个日期的失败信息。"""
    update_failure_file(fail_log_path_for_market(market), None, f"{exchange}:{market}:{symbol}:{date_str}")


def build_okx_request(url: str) -> Request:
    """构造OKX订单簿下载请求。"""
    return Request(
        url,
        headers={
            "User-Agent": HTTP_HEADER_USER_AGENT,
            "Accept": HTTP_HEADER_ACCEPT_ALL,
            "Accept-Language": HTTP_HEADER_ACCEPT_LANGUAGE,
            "Referer": HTTP_HEADER_OKX_REFERER,
            "Origin": HTTP_HEADER_OKX_ORIGIN,
        },
    )


def build_bitget_request(url: str) -> Request:
    """构造Bitget订单簿下载请求。"""
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


def is_valid_archive(output_path: Path) -> bool:
    """判断归档文件是否为有效压缩包。"""
    if output_path.name.endswith(".zip") or output_path.name.endswith(".zip.part"):
        return zipfile.is_zipfile(output_path)
    if output_path.name.endswith(".tar.gz") or output_path.name.endswith(".tar.gz.part"):
        return tarfile.is_tarfile(output_path)
    return False


def invalid_archive_message(output_path: Path) -> str:
    """返回归档校验失败提示。"""
    if output_path.name.endswith(".tar.gz") or output_path.name.endswith(".tar.gz.part"):
        return "归档不是有效tar.gz"
    return "归档不是有效zip"


def open_download_request(exchange: str, url: str):
    """打开下载请求并返回响应对象。"""
    request = url
    if exchange == "okx":
        request = build_okx_request(url)
    elif exchange == "bitget":
        request = build_bitget_request(url)
    return urlopen(request, timeout=TIMEOUT_SECONDS)


def download_archive(exchange: str, url: str, output_path: Path) -> tuple[bool, str]:
    """下载单个归档文件。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = build_part_path(output_path)
    last_error = ""
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            with open_download_request(exchange, url) as response:
                if tmp_path.exists():
                    tmp_path.unlink()
                with tmp_path.open("wb") as file_obj:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        file_obj.write(chunk)
        except HTTPError as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            if exc.code == 404:
                return False, "HTTP 404"
            if exchange == "bitget" and exc.code == 403:
                return False, "HTTP 403"
            last_error = f"HTTP {exc.code}"
        except URLError as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            last_error = f"网络错误: {exc.reason}"
        except TimeoutError:
            if tmp_path.exists():
                tmp_path.unlink()
            last_error = "下载超时"
        except socket.timeout:
            if tmp_path.exists():
                tmp_path.unlink()
            last_error = "下载超时"
        else:
            if tmp_path.stat().st_size == 0:
                tmp_path.unlink()
                last_error = "下载为空"
            elif not is_valid_archive(tmp_path):
                tmp_path.unlink()
                last_error = invalid_archive_message(output_path)
            else:
                replace_output_file(tmp_path, output_path)
                return True, ""
        if attempt < RETRY_TIMES:
            time.sleep(RETRY_INTERVAL_SECONDS)
    return False, last_error


def download_date(exchange: str, market: str, symbol: str, date_str: str) -> bool:
    """下载单个日期的订单簿归档。"""
    base_dir = data_dir_for_market(market, exchange)
    if not base_dir:
        return False
    output_path = build_output_path(exchange, market, base_dir, symbol, date_str)
    cleanup_stale_part_file(output_path)
    if output_path.exists() and is_valid_archive(output_path):
        clear_failure(exchange, market, symbol, date_str)
        return True
    url = build_url(exchange, market, symbol, date_str)
    ok, message = download_archive(exchange, url, output_path)
    if ok:
        clear_failure(exchange, market, symbol, date_str)
        return True
    record_failure(exchange, market, symbol, date_str, message)
    return False


def iter_symbol_failures(exchange: str, market: str, symbol: str) -> list[dict]:
    """筛选某个交易对的失败记录。"""
    items = []
    for record in load_failures(fail_log_path_for_market(market)):
        if record.get("交易所") != exchange:
            continue
        if record.get("市场") != market:
            continue
        if record.get("交易对") != symbol:
            continue
        items.append(record)
    return items


def sync_symbol(exchange: str, market: str, symbol: str) -> None:
    """同步单个交易对的历史订单簿归档。"""
    dataset_id = dataset_id_for_market(market)
    base_dir = data_dir_for_market(market, exchange)
    start_date = cex_config.get_start_date(dataset_id, exchange, symbol) or cex_config.get_min_start_date(dataset_id, exchange)
    if not base_dir or not start_date:
        status_update(exchange, market, symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    done_count = 0
    failures = iter_symbol_failures(exchange, market, symbol)
    for record in failures:
        date_str = str(record.get("目标") or "")
        if not date_str:
            continue
        file_name = build_output_path(exchange, market, base_dir, symbol, date_str).name
        status_update(exchange, market, symbol, (done_count, f"重试 {date_str} {file_name}"))
        if download_date(exchange, market, symbol, date_str):
            done_count += 1
    latest_local = get_latest_local_date(base_dir, exchange, market, symbol)
    start_dt = datetime.strptime(latest_local or start_date, "%Y-%m-%d")
    if latest_local:
        start_dt += timedelta(days=1)
    end_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    if start_dt > end_dt:
        status_update(exchange, market, symbol, (done_count, f"日 {latest_local or end_dt.strftime('%Y-%m-%d')} 已是最新"))
        return
    date_list = list(iter_dates(start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")))
    total = len(date_list)
    for index, date_str in enumerate(date_list, 1):
        file_name = build_output_path(exchange, market, base_dir, symbol, date_str).name
        status_update(exchange, market, symbol, (done_count, f"{index}/{total} {date_str} {file_name}"))
        if download_date(exchange, market, symbol, date_str):
            done_count += 1
            status_update(exchange, market, symbol, (done_count, f"{index}/{total} {date_str} {file_name}"))


def resolve_symbols(exchange: str, market: str) -> list[str]:
    """解析指定交易所历史订单簿交易对列表。"""
    if market == "spot":
        return cex_config.get_spot_symbols(exchange)
    symbols = set(cex_config.get_future_symbols(exchange))
    if exchange == "bybit":
        try:
            symbols.update(list_bybit_delivery_symbols_since(cex_config.get_min_start_date(dataset_id_for_market(market), "bybit")))
        except NetworkRequestError as exc:
            log(f"bybit future 动态交割合约刷新失败，继续使用静态列表: {exc}")
    return sorted(symbols)


def mark_unsupported_exchanges(market: str) -> None:
    """为未支持的交易所写入状态。"""
    dataset_id = dataset_id_for_market(market)
    for exchange in cex_config.list_exchanges():
        if cex_config.is_supported(dataset_id, exchange):
            continue
        symbol_list = cex_config.get_future_symbols(exchange) if market == "future" else cex_config.get_spot_symbols(exchange)
        for symbol in symbol_list:
            status_update(exchange, market, symbol, cex_config.UNSUPPORTED_STATUS_TEXT)


def align_to_utc_4h(market: str) -> None:
    """在首次执行前对齐到UTC四小时整点。"""
    sleep_seconds = seconds_until_next_utc_4h()
    if sleep_seconds <= 1:
        return
    log(f"{market} 启动对齐，等待 {sleep_seconds} 秒后执行（UTC 4小时倍数）")
    time.sleep(sleep_seconds)


def run_market(market: str) -> None:
    """运行指定市场的历史订单簿下载任务。"""
    aligned = False
    dataset_id = dataset_id_for_market(market)
    while True:
        if not aligned:
            align_to_utc_4h(market)
            aligned = True
        mark_unsupported_exchanges(market)
        for exchange in cex_config.get_supported_exchanges(dataset_id):
            for symbol in resolve_symbols(exchange, market):
                sync_symbol(exchange, market, symbol)
        sleep_seconds = seconds_until_next_utc_4h()
        log(f"{market} 等待 {sleep_seconds} 秒后再次执行（UTC 4小时倍数）")
        time.sleep(sleep_seconds)
