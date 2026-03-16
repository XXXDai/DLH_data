from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
import re
import socket
import tarfile
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import app_config
import orjson
from cex import cex_config
from cex import cex_orderbook_snapshot_common
from cex.cex_common import iter_dates
from cex.cex_common import count_existing_days
from cex.cex_common import get_synced_until_date
from cex.cex_common import load_failures
from cex.cex_common import list_missing_dates
from cex.cex_common import list_storage_file_names
from cex.cex_common import seconds_until_next_utc_4h
from cex.cex_common import storage_file_exists
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
FOLLOWUP_OUTPUT_DATASET_IDS = {
    "future": "D10011",  # 期货订单簿快照数据集标识，字符串
    "spot": "D10012",  # 现货订单簿快照数据集标识，字符串
}  # 市场到快照数据集映射，映射
TIMEOUT_SECONDS = app_config.DOWNLOAD_TIMEOUT_SECONDS  # 下载超时，秒
RETRY_TIMES = app_config.RETRY_TIMES  # 下载重试次数，次
RETRY_INTERVAL_SECONDS = app_config.RETRY_INTERVAL_SECONDS  # 下载重试间隔，秒
CHUNK_SIZE = app_config.CHUNK_SIZE  # 下载块大小，字节
QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数
MARKET_QUIET = {}  # 分市场静默模式映射，映射
MARKET_STATUS_HOOK = {}  # 分市场状态回调映射，映射
MARKET_LOG_HOOK = {}  # 分市场日志回调映射，映射


def configure_market_runtime(market: str, quiet: bool, status_hook, log_hook) -> None:
    """配置市场运行时回调。"""
    MARKET_QUIET[market] = quiet
    MARKET_STATUS_HOOK[market] = status_hook
    MARKET_LOG_HOOK[market] = log_hook


def log_market(market: str, message: str) -> None:
    """输出指定市场的日志消息。"""
    log_hook = MARKET_LOG_HOOK.get(market, LOG_HOOK)
    quiet = MARKET_QUIET.get(market, QUIET)
    if log_hook:
        log_hook(message)
    if not quiet:
        print(message)


def status_update(exchange: str, market: str, symbol: str, value) -> None:
    """更新统一状态键的状态值。"""
    status_hook = MARKET_STATUS_HOOK.get(market, STATUS_HOOK)
    if status_hook:
        status_hook(cex_config.get_status_key(exchange, market, symbol), value)


def dataset_id_for_market(market: str) -> str:
    """返回市场对应的数据集标识。"""
    return DATASET_IDS[market]


def followup_output_dataset_id_for_market(market: str) -> str:
    """返回市场对应的快照数据集标识。"""
    return FOLLOWUP_OUTPUT_DATASET_IDS[market]


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


def build_okx_normalized_file_name(symbol: str, date_str: str) -> str:
    """构造OKX归一化后的订单簿文件名。"""
    return f"{date_str}_{symbol}_ob400.data.zip"


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
    return base_dir / symbol / build_okx_normalized_file_name(symbol, date_str)


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
    suffix = f"_{symbol}_ob400.data.zip"
    if not file_name.endswith(suffix):
        return None
    date_text = file_name[: -len(suffix)]
    return date_text if is_valid_ymd(date_text) else None


def normalize_okx_levels(levels: list) -> list[list[str]]:
    """裁剪OKX盘口字段为价格和数量。"""
    return [[str(level[0]), str(level[1])] for level in levels if len(level) >= 2]


def build_okx_normalized_message(symbol: str, action: str, item: dict) -> dict:
    """构造OKX归一化后的Bybit样式消息。"""
    ts_ms = int(item.get("ts", "0") or 0)
    update_id = int(item.get("seqId", item.get("ts", "0")) or 0)
    previous_id = int(item.get("prevSeqId", update_id) or update_id)
    return {
        "topic": f"orderbook.400.{symbol}",
        "type": "snapshot" if action == "snapshot" else "delta",
        "ts": ts_ms,
        "data": {
            "s": symbol,
            "b": normalize_okx_levels(item.get("bids", [])),
            "a": normalize_okx_levels(item.get("asks", [])),
            "u": update_id,
            "seq": previous_id,
        },
        "cts": ts_ms,
    }


def convert_okx_tar_to_zip(raw_path: Path, output_path: Path, symbol: str) -> None:
    """将OKX原始tar归一化为Bybit样式zip。"""
    inner_name = f"{symbol}.json"
    with tarfile.open(raw_path, "r:gz") as tar_file, zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        with zip_file.open(inner_name, "w") as zip_obj:
            for member in tar_file.getmembers():
                if not member.isfile():
                    continue
                file_obj = tar_file.extractfile(member)
                if not file_obj:
                    continue
                for line in file_obj:
                    message = orjson.loads(line)
                    action = message.get("action", "")
                    if action not in {"snapshot", "update"}:
                        continue
                    for item in message.get("data", []):
                        normalized = build_okx_normalized_message(message.get("arg", {}).get("instId", symbol), action, item)
                        zip_obj.write(orjson.dumps(normalized))
                        zip_obj.write(b"\n")


def download_okx_normalized(url: str, output_path: Path, symbol: str) -> tuple[bool, str]:
    """下载并归一化OKX订单簿归档。"""
    raw_tmp_path = output_path.with_name(output_path.name + ".tar.gz.part")
    zip_tmp_path = build_part_path(output_path)
    last_error = ""
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            with open_download_request("okx", url) as response:
                if raw_tmp_path.exists():
                    raw_tmp_path.unlink()
                with raw_tmp_path.open("wb") as file_obj:
                    while True:
                        chunk = response.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        file_obj.write(chunk)
        except HTTPError as exc:
            if raw_tmp_path.exists():
                raw_tmp_path.unlink()
            if exc.code == 404:
                return False, "HTTP 404"
            last_error = f"HTTP {exc.code}"
        except URLError as exc:
            if raw_tmp_path.exists():
                raw_tmp_path.unlink()
            last_error = f"网络错误: {exc.reason}"
        except TimeoutError:
            if raw_tmp_path.exists():
                raw_tmp_path.unlink()
            last_error = "下载超时"
        except socket.timeout:
            if raw_tmp_path.exists():
                raw_tmp_path.unlink()
            last_error = "下载超时"
        else:
            if raw_tmp_path.stat().st_size == 0:
                raw_tmp_path.unlink()
                last_error = "下载为空"
            elif not tarfile.is_tarfile(raw_tmp_path):
                raw_tmp_path.unlink()
                last_error = "归档不是有效tar.gz"
            else:
                if zip_tmp_path.exists():
                    zip_tmp_path.unlink()
                convert_okx_tar_to_zip(raw_tmp_path, zip_tmp_path, symbol)
                raw_tmp_path.unlink()
                if not zipfile.is_zipfile(zip_tmp_path):
                    zip_tmp_path.unlink()
                    last_error = "归档不是有效zip"
                else:
                    replace_output_file(zip_tmp_path, output_path)
                    return True, ""
        if attempt < RETRY_TIMES:
            time.sleep(RETRY_INTERVAL_SECONDS)
    return False, last_error


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
    yield from symbol_dir.glob("*_ob400.data.zip")


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


def get_existing_dates(base_dir: Path, exchange: str, market: str, symbol: str) -> set[str]:
    """获取存储中已经存在的订单簿日期集合。"""
    dates = set()
    for file_name in list_storage_file_names(base_dir / symbol):
        date_text = parse_date_from_name(exchange, market, file_name, symbol)
        if date_text:
            dates.add(date_text)
    return dates


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
    if storage_file_exists(output_path) and (not output_path.exists() or is_valid_archive(output_path)):
        clear_failure(exchange, market, symbol, date_str)
        return True
    url = build_url(exchange, market, symbol, date_str)
    if exchange == "okx":
        ok, message = download_okx_normalized(url, output_path, symbol)
    else:
        ok, message = download_archive(exchange, url, output_path)
    if ok:
        clear_failure(exchange, market, symbol, date_str)
        return True
    record_failure(exchange, market, symbol, date_str, message)
    return False


def process_followup_snapshot(exchange: str, market: str, symbol: str, date_str: str) -> None:
    """处理订单簿归档对应的快照输出。"""
    input_dataset_id = dataset_id_for_market(market)
    output_dataset_id = followup_output_dataset_id_for_market(market)
    if not cex_config.is_supported(output_dataset_id, exchange):
        return
    cex_orderbook_snapshot_common.process_single_date(input_dataset_id, output_dataset_id, exchange, symbol, date_str)


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
    end_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=1)
    end_date = end_dt.strftime("%Y-%m-%d")
    existing_dates = get_existing_dates(base_dir, exchange, market, symbol)
    done_count = 0
    if end_dt >= datetime.strptime(start_date, "%Y-%m-%d"):
        done_count = count_existing_days(existing_dates, start_date, end_date)
    failures = iter_symbol_failures(exchange, market, symbol)
    for record in failures:
        date_str = str(record.get("目标") or "")
        if not date_str:
            continue
        file_name = build_output_path(exchange, market, base_dir, symbol, date_str).name
        status_update(exchange, market, symbol, (done_count, f"重试 {date_str} {file_name}"))
        log_market(market, f"{exchange} {market} {symbol} 重试日包: {date_str}")
        if download_date(exchange, market, symbol, date_str):
            process_followup_snapshot(exchange, market, symbol, date_str)
            done_count += 1
            existing_dates.add(date_str)
        else:
            status_update(exchange, market, symbol, (done_count, f"失败 {date_str}"))
    if end_dt < datetime.strptime(start_date, "%Y-%m-%d"):
        status_update(exchange, market, symbol, (done_count, f"日 {start_date} 已是最新"))
        return
    date_list = list_missing_dates(existing_dates, start_date, end_date)
    if not date_list:
        synced_until = get_synced_until_date(existing_dates, start_date, end_date) or end_date
        status_update(exchange, market, symbol, (done_count, f"日 {synced_until} 已是最新"))
        return
    next_date = date_list[0]
    if done_count > 0:
        synced_until = get_synced_until_date(existing_dates, start_date, end_date)
        if synced_until:
            status_update(exchange, market, symbol, (done_count, f"日 {synced_until} 准备回补"))
    else:
        status_update(exchange, market, symbol, (done_count, f"准备 {next_date}"))
    log_market(market, f"{exchange} {market} {symbol} 开始回补: {next_date} -> {end_date}")
    total = len(date_list)
    for index, date_str in enumerate(date_list, 1):
        file_name = build_output_path(exchange, market, base_dir, symbol, date_str).name
        status_update(exchange, market, symbol, (done_count, f"{index}/{total} {date_str} 请求中"))
        log_market(market, f"{exchange} {market} {symbol} 请求日包: {date_str}")
        if download_date(exchange, market, symbol, date_str):
            process_followup_snapshot(exchange, market, symbol, date_str)
            done_count += 1
            existing_dates.add(date_str)
            status_update(exchange, market, symbol, (done_count, f"{index}/{total} {date_str} {file_name}"))
        else:
            status_update(exchange, market, symbol, (done_count, f"失败 {date_str}"))


def resolve_symbols(exchange: str, market: str) -> list[str]:
    """解析指定交易所历史订单簿交易对列表。"""
    if market == "spot":
        return cex_config.get_spot_symbols(exchange)
    symbols = set(cex_config.get_future_symbols(exchange))
    if exchange == "bybit":
        try:
            symbols.update(list_bybit_delivery_symbols_since(cex_config.get_min_start_date(dataset_id_for_market(market), "bybit")))
        except NetworkRequestError as exc:
            log_market(market, f"bybit future 动态交割合约刷新失败，继续使用静态列表: {exc}")
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
    log_market(market, f"{market} 启动对齐，等待 {sleep_seconds} 秒后执行（UTC 4小时倍数）")
    time.sleep(sleep_seconds)


def run_market(market: str) -> None:
    """运行指定市场的历史订单簿下载任务。"""
    dataset_id = dataset_id_for_market(market)
    while True:
        mark_unsupported_exchanges(market)
        for exchange in cex_config.get_supported_exchanges(dataset_id):
            for symbol in resolve_symbols(exchange, market):
                sync_symbol(exchange, market, symbol)
        sleep_seconds = seconds_until_next_utc_4h()
        log_market(market, f"{market} 等待 {sleep_seconds} 秒后再次执行（UTC 4小时倍数）")
        time.sleep(sleep_seconds)
