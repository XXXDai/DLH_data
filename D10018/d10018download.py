from datetime import datetime, timezone
from pathlib import Path
import csv
from decimal import Decimal
import json
import socket
import threading
from urllib.error import HTTPError
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen

import app_config
from cex import cex_config
from cex.cex_common import seconds_until_next_utc_midnight
from cex.cex_common import upload_file_to_s3
from cex.cex_orderbook_ws_common import NetworkRequestError


DATASET_ID = "D10018"  # 数据集标识，字符串
TIMEOUT_SECONDS = app_config.HTTP_TIMEOUT_SECONDS  # 请求超时，秒
BINANCE_API_URL = "https://fapi.binance.com/fapi/v1/insuranceBalance"  # Binance保险基金接口地址，字符串
BITGET_BTC_TICKER_URL = "https://api.bitget.com/api/v2/spot/market/tickers?symbol=BTCUSDT"  # Bitget现货BTC价格接口地址，字符串
BITGET_BTC_ADDRESS_API_URL = "https://blockstream.info/api/address"  # Bitget链上地址接口地址，字符串
BITGET_BTC_ADDRESSES = [  # Bitget保险基金地址列表，个数
    "3H6JnFoz5jcoATKQ83BuQ3cUUCHswqfgtG",  # Bitget保险基金地址一，字符串
    "3KUwtHc5UhWQ76z6WrZRQHHVTZMuUWiZcU",  # Bitget保险基金地址二，字符串
    "3AZHcgLnJL5C5xKo33mspyHpQX7x4H5bBw",  # Bitget保险基金地址三，字符串
]  # Bitget保险基金地址列表，个数
BYBIT_API_URL = "https://api.bybit.com/v5/market/insurance"  # Bybit保险基金接口地址，字符串
OKX_API_URL = "https://www.okx.com/api/v5/public/insurance-fund"  # OKX保险基金接口地址，字符串
OKX_INST_TYPES = ["SWAP", "FUTURES"]  # OKX保险基金产品类型列表，个数
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
    """构造保险基金文件路径。"""
    return base_dir / symbol / f"{symbol}_insurance.csv"


def request_json(url: str) -> dict:
    """请求JSON响应。"""
    request = Request(url, headers={"User-Agent": HTTP_HEADER_USER_AGENT})
    try:
        with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise NetworkRequestError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise NetworkRequestError("接口请求失败: 网络错误") from exc
    except TimeoutError as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc
    except socket.timeout as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc


def format_date_fields(ts_ms: int) -> tuple[str, str, str]:
    """格式化事件时间字段。"""
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return str(ts_ms), dt.strftime("%Y-%m-%d"), dt.strftime("%Y-%m-%d %H:%M:%S")


def collect_fields() -> tuple[str, str, str]:
    """生成采集时间字段。"""
    now = datetime.now(tz=timezone.utc)
    return str(int(now.timestamp() * 1000)), now.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d %H:%M:%S")


def append_rows(file_path: Path, rows: list) -> int:
    """追加写入保险基金记录。"""
    if not rows:
        return 0
    file_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not file_path.exists()
    with file_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "pool_type",
                "contract_type",
                "pool_symbol_list",
                "pool_symbol_num",
                "pool_balance",
                "pool_value",
                "coin",
                "event_ts",
                "event_dt",
                "event_time",
                "collect_ts",
                "collect_date",
                "collect_time",
            ],
        )
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    upload_file_to_s3(file_path)
    return len(rows)


def fetch_bybit_rows(coin: str) -> list:
    """抓取Bybit保险基金记录。"""
    payload = request_json(f"{BYBIT_API_URL}?{urlencode({'coin': coin})}")
    if payload.get("retCode") != 0:
        raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
    result = payload.get("result", {})
    items = result.get("list", [])
    event_ts_text = str(result.get("updatedTime", "0") or "0")
    event_ts = int(event_ts_text)
    event_ts_text, event_dt, event_time = format_date_fields(event_ts)
    collect_ts, collect_date, collect_time = collect_fields()
    rows = []
    for item in items:
        symbols_text = item.get("symbols", "")
        symbols = [value for value in symbols_text.split(",") if value]
        pool_type = "shared" if len(symbols) > 1 else "dedicated"
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol,
                    "pool_type": pool_type,
                    "contract_type": "futures" if "-" in symbol else "perpetual",
                    "pool_symbol_list": symbols_text,
                    "pool_symbol_num": str(len(symbols)),
                    "pool_balance": item.get("balance", ""),
                    "pool_value": item.get("value", ""),
                    "coin": item.get("coin", coin),
                    "event_ts": event_ts_text,
                    "event_dt": event_dt,
                    "event_time": event_time,
                    "collect_ts": collect_ts,
                    "collect_date": collect_date,
                    "collect_time": collect_time,
                }
            )
    return rows


def fetch_binance_rows(symbol: str) -> list:
    """抓取Binance保险基金记录。"""
    payload = request_json(f"{BINANCE_API_URL}?{urlencode({'symbol': symbol})}")
    if isinstance(payload, dict):
        payload = [payload]
    if not payload:
        return []
    event_ts = max(
        int(asset.get("updateTime", "0") or "0")
        for item in payload
        for asset in item.get("assets", [])
    )
    event_ts_text, event_dt, event_time = format_date_fields(event_ts)
    collect_ts, collect_date, collect_time = collect_fields()
    rows = []
    for item in payload:
        symbol_list = item.get("symbols", [])
        pool_type = "shared" if len(symbol_list) > 1 else "dedicated"
        pool_symbol_list = ",".join(symbol_list)
        contract_type = "futures" if any("_" in item_symbol for item_symbol in symbol_list) else "perpetual"
        for asset in item.get("assets", []):
            rows.append(
                {
                    "symbol": symbol,
                    "pool_type": pool_type,
                    "contract_type": contract_type,
                    "pool_symbol_list": pool_symbol_list,
                    "pool_symbol_num": str(len(symbol_list)),
                    "pool_balance": asset.get("marginBalance", ""),
                    "pool_value": asset.get("marginBalance", ""),
                    "coin": asset.get("asset", ""),
                    "event_ts": event_ts_text,
                    "event_dt": event_dt,
                    "event_time": event_time,
                    "collect_ts": collect_ts,
                    "collect_date": collect_date,
                    "collect_time": collect_time,
                }
            )
    return rows


def format_decimal(value: Decimal, scale: str) -> str:
    """格式化小数字符串。"""
    return format(value.quantize(Decimal(scale)), "f")


def fetch_bitget_wallet_balance(address: str) -> Decimal:
    """抓取Bitget单个钱包余额。"""
    payload = request_json(f"{BITGET_BTC_ADDRESS_API_URL}/{address}")
    chain_stats = payload.get("chain_stats", {})
    mempool_stats = payload.get("mempool_stats", {})
    funded_sum = int(chain_stats.get("funded_txo_sum", 0) or 0) + int(mempool_stats.get("funded_txo_sum", 0) or 0)
    spent_sum = int(chain_stats.get("spent_txo_sum", 0) or 0) + int(mempool_stats.get("spent_txo_sum", 0) or 0)
    return Decimal(funded_sum - spent_sum) / Decimal("100000000")


def fetch_bitget_btc_price() -> Decimal:
    """抓取Bitget现货BTC价格。"""
    payload = request_json(BITGET_BTC_TICKER_URL)
    if payload.get("code") != "00000":
        raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
    items = payload.get("data", [])
    if not items:
        raise RuntimeError("接口返回错误: 缺少价格数据")
    return Decimal(items[0].get("lastPr", "0") or "0")


def fetch_bitget_rows(symbol: str) -> list:
    """抓取Bitget保险基金记录。"""
    collect_ts, collect_date, collect_time = collect_fields()
    total_balance = sum((fetch_bitget_wallet_balance(address) for address in BITGET_BTC_ADDRESSES), Decimal("0"))
    btc_price = fetch_bitget_btc_price()
    return [
        {
            "symbol": symbol,
            "pool_type": "wallet",
            "contract_type": "wallet",
            "pool_symbol_list": ",".join(BITGET_BTC_ADDRESSES),
            "pool_symbol_num": str(len(BITGET_BTC_ADDRESSES)),
            "pool_balance": format_decimal(total_balance, "0.00000001"),
            "pool_value": format_decimal(total_balance * btc_price, "0.00000001"),
            "coin": "BTC",
            "event_ts": collect_ts,
            "event_dt": collect_date,
            "event_time": collect_time,
            "collect_ts": collect_ts,
            "collect_date": collect_date,
            "collect_time": collect_time,
        }
    ]


def fetch_okx_rows(inst_family: str) -> list:
    """抓取OKX保险基金记录。"""
    rows = []
    collect_ts, collect_date, collect_time = collect_fields()
    for inst_type in OKX_INST_TYPES:
        query = urlencode({"instType": inst_type, "instFamily": inst_family})
        payload = request_json(f"{OKX_API_URL}?{query}")
        if payload.get("code") != "0":
            raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
        items = payload.get("data", [])
        if not items:
            continue
        details = items[0].get("details", [])
        if not details:
            continue
        latest = max(details, key=lambda item: int(item.get("ts", "0") or "0"))
        event_ts = int(latest.get("ts", "0") or "0")
        event_ts_text, event_dt, event_time = format_date_fields(event_ts)
        rows.append(
            {
                "symbol": inst_family,
                "pool_type": "family",
                "contract_type": "perpetual" if inst_type == "SWAP" else "futures",
                "pool_symbol_list": inst_family,
                "pool_symbol_num": "1",
                "pool_balance": latest.get("balance", ""),
                "pool_value": latest.get("balance", ""),
                "coin": latest.get("ccy", "USDT"),
                "event_ts": event_ts_text,
                "event_dt": event_dt,
                "event_time": event_time,
                "collect_ts": collect_ts,
                "collect_date": collect_date,
                "collect_time": collect_time,
            }
        )
    return rows


def run_bybit() -> None:
    """执行Bybit保险基金同步。"""
    exchange = "bybit"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for symbol in cex_config.get_insurance_symbols(exchange):
            status_update(exchange, "insurance", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    for coin in cex_config.get_insurance_symbols(exchange):
        try:
            rows = fetch_bybit_rows(coin)
        except NetworkRequestError as exc:
            status_update(exchange, "insurance", coin, f"失败 {exc}")
            log(f"{exchange} {coin} 同步失败: {exc}")
            continue
        count = append_rows(build_file_path(base_dir, coin), rows)
        status_update(exchange, "insurance", coin, count)
        log(f"{exchange} {coin} 已写入记录数: {count}")


def run_binance() -> None:
    """执行Binance保险基金同步。"""
    exchange = "binance"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for symbol in cex_config.get_insurance_symbols(exchange):
            status_update(exchange, "insurance", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    for symbol in cex_config.get_insurance_symbols(exchange):
        try:
            rows = fetch_binance_rows(symbol)
        except NetworkRequestError as exc:
            status_update(exchange, "insurance", symbol, f"失败 {exc}")
            log(f"{exchange} {symbol} 同步失败: {exc}")
            continue
        count = append_rows(build_file_path(base_dir, symbol), rows)
        status_update(exchange, "insurance", symbol, count)
        log(f"{exchange} {symbol} 已写入记录数: {count}")


def run_bitget() -> None:
    """执行Bitget保险基金同步。"""
    exchange = "bitget"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for symbol in cex_config.get_insurance_symbols(exchange):
            status_update(exchange, "insurance", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    for symbol in cex_config.get_insurance_symbols(exchange):
        try:
            rows = fetch_bitget_rows(symbol)
        except NetworkRequestError as exc:
            status_update(exchange, "insurance", symbol, f"失败 {exc}")
            log(f"{exchange} {symbol} 同步失败: {exc}")
            continue
        count = append_rows(build_file_path(base_dir, symbol), rows)
        status_update(exchange, "insurance", symbol, count)
        log(f"{exchange} {symbol} 已写入记录数: {count}")


def run_okx() -> None:
    """执行OKX保险基金同步。"""
    exchange = "okx"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for symbol in cex_config.get_insurance_symbols(exchange):
            status_update(exchange, "insurance", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    for symbol in cex_config.get_insurance_symbols(exchange):
        try:
            rows = fetch_okx_rows(symbol)
        except NetworkRequestError as exc:
            status_update(exchange, "insurance", symbol, f"失败 {exc}")
            log(f"{exchange} {symbol} 同步失败: {exc}")
            continue
        count = append_rows(build_file_path(base_dir, symbol), rows)
        status_update(exchange, "insurance", symbol, count)
        log(f"{exchange} {symbol} 已写入记录数: {count}")


def mark_unsupported(exchange: str) -> None:
    """标记不支持的交易所状态。"""
    symbols = cex_config.get_insurance_symbols(exchange) or cex_config.get_future_symbols(exchange)
    for symbol in symbols:
        status_update(exchange, "insurance", symbol, cex_config.UNSUPPORTED_STATUS_TEXT)


def main() -> None:
    """运行保险基金主循环。"""
    while True:
        threads = [
            threading.Thread(target=run_bybit, daemon=True),
            threading.Thread(target=run_binance, daemon=True),
            threading.Thread(target=run_bitget, daemon=True),
            threading.Thread(target=run_okx, daemon=True),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        sleep_seconds = seconds_until_next_utc_midnight()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 00:00）")
        threading.Event().wait(sleep_seconds)


def run() -> None:
    """兼容启动器运行入口。"""
    main()


if __name__ == "__main__":
    run()
