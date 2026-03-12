from datetime import datetime, timezone
from pathlib import Path
import base64
import csv
import hashlib
import hmac
import json
import threading
import time
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import app_config
from cex import cex_config
from cex.cex_common import download_file_from_storage
from cex.cex_common import seconds_until_next_utc_midnight
from cex.cex_common import upload_file_to_s3


DATASET_ID = "D10019"  # 数据集标识，字符串
TIMEOUT_SECONDS = app_config.HTTP_TIMEOUT_SECONDS  # 请求超时，秒
BYBIT_API_URL = "https://api.bybit.com/v5/earn/product"  # Bybit理财接口地址，字符串
BYBIT_CATEGORY = "OnChain"  # Bybit理财产品类型，字符串
BINANCE_API_URL = "https://api.binance.com/sapi/v1/simple-earn/flexible/list"  # Binance理财接口地址，字符串
BINANCE_API_KEY = "HcoGbXARqmRcCxWTjmtraQLWcPe0T4S1m55TUEmFZvVAFbPPUdVEvsNrSvB7kWia"  # Binance只读接口密钥，字符串
BINANCE_SECRET_KEY = "HP2mMjmUXIeaD9dSnP2RrdudUfEUBagzJcqnCJG0MKgPQwFN51meo972ArVWsK3a"  # Binance只读接口私钥，字符串
BINANCE_PAGE_SIZE = 100  # Binance理财分页大小，条
BINANCE_RECV_WINDOW = 5000  # Binance鉴权窗口，毫秒
BITGET_API_BASE_URL = "https://api.bitget.com"  # Bitget接口基础地址，字符串
BITGET_API_KEY = "bg_33ed24997e71359e737d5dbe0f65de20"  # Bitget只读接口密钥，字符串
BITGET_SECRET_KEY = "d8a33ac73a09480a783e27a8f6f8dd29e9a1f23d972492f36dd85ab721cdcad7"  # Bitget只读接口私钥，字符串
BITGET_PASSPHRASE = "daijunling1"  # Bitget接口口令，字符串
BITGET_PRODUCTS_PATH = "/api/v2/earn/savings/product"  # Bitget理财产品列表路径，字符串
BITGET_PRODUCT_FILTER = "available"  # Bitget理财产品筛选类型，字符串
BITGET_PRIVATE_BASE_HEADERS = {"locale": "en-US", "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}  # Bitget私有接口基础请求头，映射
OKX_API_BASE_URL = "https://www.okx.com"  # OKX接口基础地址，字符串
OKX_API_KEY = "8a248738-48e3-4b9a-8123-c964b6362175"  # OKX只读接口密钥，字符串
OKX_SECRET_KEY = "65BA5BCB3619559DF744DDF0A8E00CD9"  # OKX只读接口私钥，字符串
OKX_PASSPHRASE = "STYnb1999117@"  # OKX接口口令，字符串
OKX_OFFERS_PATH = "/api/v5/finance/staking-defi/offers"  # OKX链上产品列表路径，字符串
OKX_ETH_PRODUCT_INFO_PATH = "/api/v5/finance/staking-defi/eth/product-info"  # OKX以太坊产品信息路径，字符串
OKX_SOL_PRODUCT_INFO_PATH = "/api/v5/finance/staking-defi/sol/product-info"  # OKX索拉纳产品信息路径，字符串
OKX_PRIVATE_BASE_HEADERS = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"}  # OKX私有接口基础请求头，映射
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


def build_file_path(base_dir: Path, coin: str) -> Path:
    """构造理财文件路径。"""
    return base_dir / coin / f"{coin}_onchainstaking.csv"


def request_json(url: str, headers: dict | None = None) -> dict:
    """请求JSON响应。"""
    request = Request(url, headers=headers or {})
    with urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


def normalize_apr(apr_text: str) -> str:
    """标准化APR文本。"""
    text = (apr_text or "").strip()
    if not text:
        return ""
    if text.endswith("%"):
        value = float(text.rstrip("%")) / 100
        return f"{value:.6f}".rstrip("0").rstrip(".")
    return text


def append_rows(file_path: Path, rows: list) -> int:
    """追加写入理财记录。"""
    if not rows:
        return 0
    file_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not file_path.exists()
    with file_path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "coin",
                "lst",
                "apr",
                "product_id",
                "stake_rate",
                "redeem_rate",
                "min_stake",
                "max_stake",
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


def load_existing_sync_info(file_path: Path) -> tuple[int, str]:
    """读取现有记录数量与最新采集日期。"""
    if not file_path.exists():
        download_file_from_storage(file_path)
    if not file_path.exists():
        return 0, ""
    count = 0
    latest_date = ""
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            count += 1
            collect_date = str(row.get("collect_date") or "")
            if collect_date > latest_date:
                latest_date = collect_date
    return count, latest_date


def build_binance_signed_url(current: int) -> str:
    """构造Binance签名请求地址。"""
    params = {
        "timestamp": int(time.time() * 1000),
        "recvWindow": BINANCE_RECV_WINDOW,
        "current": current,
        "size": BINANCE_PAGE_SIZE,
    }
    query = urlencode(params)
    signature = hmac.new(BINANCE_SECRET_KEY.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{BINANCE_API_URL}?{query}&signature={signature}"


def build_okx_auth_headers(request_path: str) -> dict:
    """构造OKX鉴权请求头。"""
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    prehash = f"{timestamp}GET{request_path}"
    signature = base64.b64encode(
        hmac.new(OKX_SECRET_KEY.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    headers = dict(OKX_PRIVATE_BASE_HEADERS)
    headers["OK-ACCESS-KEY"] = OKX_API_KEY
    headers["OK-ACCESS-SIGN"] = signature
    headers["OK-ACCESS-TIMESTAMP"] = timestamp
    headers["OK-ACCESS-PASSPHRASE"] = OKX_PASSPHRASE
    return headers


def build_bitget_auth_headers(request_path: str, query_string: str) -> dict:
    """构造Bitget鉴权请求头。"""
    timestamp = str(int(time.time() * 1000))
    suffix = f"?{query_string}" if query_string else ""
    prehash = f"{timestamp}GET{request_path}{suffix}"
    signature = base64.b64encode(
        hmac.new(BITGET_SECRET_KEY.encode("utf-8"), prehash.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")
    headers = dict(BITGET_PRIVATE_BASE_HEADERS)
    headers["ACCESS-KEY"] = BITGET_API_KEY
    headers["ACCESS-SIGN"] = signature
    headers["ACCESS-TIMESTAMP"] = timestamp
    headers["ACCESS-PASSPHRASE"] = BITGET_PASSPHRASE
    return headers


def request_okx_private_json(request_path: str) -> dict:
    """请求OKX私有JSON响应。"""
    url = f"{OKX_API_BASE_URL}{request_path}"
    return request_json(url, build_okx_auth_headers(request_path))


def request_bitget_private_json(request_path: str, params: dict | None = None) -> dict:
    """请求Bitget私有JSON响应。"""
    query_string = urlencode(params or {})
    suffix = f"?{query_string}" if query_string else ""
    url = f"{BITGET_API_BASE_URL}{request_path}{suffix}"
    return request_json(url, build_bitget_auth_headers(request_path, query_string))


def fetch_bybit_rows() -> dict:
    """抓取Bybit理财记录。"""
    payload = request_json(f"{BYBIT_API_URL}?{urlencode({'category': BYBIT_CATEGORY})}")
    if payload.get("retCode") != 0:
        raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
    now = datetime.now(tz=timezone.utc)
    collect_ts = str(int(now.timestamp() * 1000))
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    rows_by_coin = {}
    for item in payload.get("result", {}).get("list", []):
        coin = item.get("coin", "")
        if coin not in cex_config.get_earn_coins("bybit"):
            continue
        rows_by_coin.setdefault(coin, []).append(
            {
                "coin": coin,
                "lst": item.get("swapCoin", ""),
                "apr": normalize_apr(item.get("estimateApr", "")),
                "product_id": item.get("productId", ""),
                "stake_rate": item.get("stakeExchangeRate", ""),
                "redeem_rate": item.get("redeemExchangeRate", ""),
                "min_stake": item.get("minStakeAmount", ""),
                "max_stake": item.get("maxStakeAmount", ""),
                "collect_ts": collect_ts,
                "collect_date": collect_date,
                "collect_time": collect_time,
            }
        )
    return rows_by_coin


def fetch_binance_rows() -> dict:
    """抓取Binance理财记录。"""
    now = datetime.now(tz=timezone.utc)
    collect_ts = str(int(now.timestamp() * 1000))
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    rows_by_coin = {}
    current = 1
    while True:
        payload = request_json(build_binance_signed_url(current), {"X-MBX-APIKEY": BINANCE_API_KEY})
        rows = payload.get("rows", [])
        total = int(payload.get("total", 0) or 0)
        for item in rows:
            coin = item.get("asset", "")
            if coin not in cex_config.get_earn_coins("binance"):
                continue
            tier_rates = item.get("tierAnnualPercentageRate", {})
            rows_by_coin.setdefault(coin, []).append(
                {
                    "coin": coin,
                    "lst": item.get("status", ""),
                    "apr": item.get("latestAnnualPercentageRate", ""),
                    "product_id": item.get("productId", ""),
                    "stake_rate": json.dumps(tier_rates, ensure_ascii=False, sort_keys=True) if tier_rates else "",
                    "redeem_rate": "1" if item.get("canRedeem") else "0",
                    "min_stake": item.get("minPurchaseAmount", ""),
                    "max_stake": "",
                    "collect_ts": collect_ts,
                    "collect_date": collect_date,
                    "collect_time": collect_time,
                }
            )
        if not rows or current * BINANCE_PAGE_SIZE >= total:
            break
        current += 1
    return rows_by_coin


def normalize_percent_value(rate_text: str) -> str:
    """标准化百分数字符串。"""
    text = (rate_text or "").strip()
    if not text:
        return ""
    return f"{float(text) / 100:.8f}".rstrip("0").rstrip(".")


def build_bitget_period_text(item: dict) -> str:
    """构造Bitget产品周期文本。"""
    period_type = item.get("periodType", "")
    period = item.get("period", "")
    return f"{period_type}_{period}d" if period else period_type


def fetch_bitget_rows() -> dict:
    """抓取Bitget理财记录。"""
    now = datetime.now(tz=timezone.utc)
    collect_ts = str(int(now.timestamp() * 1000))
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    rows_by_coin = {}
    for coin in cex_config.get_earn_coins("bitget"):
        payload = request_bitget_private_json(BITGET_PRODUCTS_PATH, {"coin": coin, "filter": BITGET_PRODUCT_FILTER})
        if payload.get("code") != "00000":
            raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
        items = payload.get("data", [])
        rows = []
        for item in items:
            apy_list = item.get("apyList", [])
            if not apy_list:
                continue
            rows.append(
                {
                    "coin": coin,
                    "lst": build_bitget_period_text(item),
                    "apr": normalize_percent_value(apy_list[0].get("currentApy", "")),
                    "product_id": item.get("productId", ""),
                    "stake_rate": json.dumps(apy_list, ensure_ascii=False, sort_keys=True),
                    "redeem_rate": item.get("advanceRedeem", ""),
                    "min_stake": apy_list[0].get("minStepVal", ""),
                    "max_stake": apy_list[-1].get("maxStepVal", ""),
                    "collect_ts": collect_ts,
                    "collect_date": collect_date,
                    "collect_time": collect_time,
                }
            )
        rows_by_coin[coin] = rows
    return rows_by_coin


def fetch_okx_rows() -> dict:
    """抓取OKX链上产品记录。"""
    now = datetime.now(tz=timezone.utc)
    collect_ts = str(int(now.timestamp() * 1000))
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    payload = request_okx_private_json(OKX_OFFERS_PATH)
    if payload.get("code") != "0":
        raise RuntimeError(f"接口返回错误: {payload.get('msg')}")
    product_info_map = {
        "ETH": request_okx_private_json(OKX_ETH_PRODUCT_INFO_PATH).get("data", [{}])[0],
        "SOL": request_okx_private_json(OKX_SOL_PRODUCT_INFO_PATH).get("data", {}),
    }
    rows_by_coin = {}
    for item in payload.get("data", []):
        coin = item.get("ccy", "")
        if coin not in cex_config.get_earn_coins("okx"):
            continue
        invest_data = item.get("investData", [{}])
        earning_data = item.get("earningData", [])
        product_info = product_info_map.get(coin, {})
        earning_ccys = [value.get("ccy", "") for value in earning_data if value.get("ccy", "")]
        fast_limit = product_info.get("fastRedemptionDailyLimit", item.get("fastRedemptionDailyLimit", ""))
        rows_by_coin.setdefault(coin, []).append(
            {
                "coin": coin,
                "lst": ",".join(earning_ccys),
                "apr": item.get("apy", ""),
                "product_id": item.get("productId", ""),
                "stake_rate": item.get("protocol", ""),
                "redeem_rate": fast_limit,
                "min_stake": invest_data[0].get("minAmt", ""),
                "max_stake": invest_data[0].get("maxAmt", ""),
                "collect_ts": collect_ts,
                "collect_date": collect_date,
                "collect_time": collect_time,
            }
        )
    return rows_by_coin


def run_bybit() -> None:
    """执行Bybit理财同步。"""
    base_dir = cex_config.get_source_dir(DATASET_ID, "bybit")
    rows_by_coin = fetch_bybit_rows()
    for coin in cex_config.get_earn_coins("bybit"):
        rows = rows_by_coin.get(coin, [])
        file_path = build_file_path(base_dir, coin)
        existing_count, latest_date = load_existing_sync_info(file_path)
        if latest_date:
            status_update("bybit", "earn", coin, (existing_count, f"日 {latest_date} 准备同步"))
        else:
            status_update("bybit", "earn", coin, (existing_count, "准备同步"))
        count = append_rows(file_path, rows)
        synced_date = rows[0].get("collect_date", "") if rows else latest_date
        status_update("bybit", "earn", coin, (existing_count + count, f"日 {synced_date} 已完成"))
        log(f"bybit {coin} 已写入记录数: {count}")


def run_binance() -> None:
    """执行Binance理财同步。"""
    exchange = "binance"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for coin in cex_config.CEX_BASE_COINS:
            status_update(exchange, "earn", coin, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    rows_by_coin = fetch_binance_rows()
    for coin in cex_config.get_earn_coins(exchange):
        rows = rows_by_coin.get(coin, [])
        file_path = build_file_path(base_dir, coin)
        existing_count, latest_date = load_existing_sync_info(file_path)
        if latest_date:
            status_update(exchange, "earn", coin, (existing_count, f"日 {latest_date} 准备同步"))
        else:
            status_update(exchange, "earn", coin, (existing_count, "准备同步"))
        count = append_rows(file_path, rows)
        synced_date = rows[0].get("collect_date", "") if rows else latest_date
        status_update(exchange, "earn", coin, (existing_count + count, f"日 {synced_date} 已完成"))
        log(f"{exchange} {coin} 已写入记录数: {count}")


def run_bitget() -> None:
    """执行Bitget理财同步。"""
    exchange = "bitget"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for coin in cex_config.CEX_BASE_COINS:
            status_update(exchange, "earn", coin, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    rows_by_coin = fetch_bitget_rows()
    for coin in cex_config.get_earn_coins(exchange):
        rows = rows_by_coin.get(coin, [])
        file_path = build_file_path(base_dir, coin)
        existing_count, latest_date = load_existing_sync_info(file_path)
        if latest_date:
            status_update(exchange, "earn", coin, (existing_count, f"日 {latest_date} 准备同步"))
        else:
            status_update(exchange, "earn", coin, (existing_count, "准备同步"))
        count = append_rows(file_path, rows)
        synced_date = rows[0].get("collect_date", "") if rows else latest_date
        status_update(exchange, "earn", coin, (existing_count + count, f"日 {synced_date} 已完成"))
        log(f"{exchange} {coin} 已写入记录数: {count}")


def run_okx() -> None:
    """执行OKX链上同步。"""
    exchange = "okx"
    if not cex_config.is_supported(DATASET_ID, exchange):
        for coin in cex_config.CEX_BASE_COINS:
            status_update(exchange, "earn", coin, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    base_dir = cex_config.get_source_dir(DATASET_ID, exchange)
    rows_by_coin = fetch_okx_rows()
    for coin in cex_config.get_earn_coins(exchange):
        rows = rows_by_coin.get(coin, [])
        file_path = build_file_path(base_dir, coin)
        existing_count, latest_date = load_existing_sync_info(file_path)
        if latest_date:
            status_update(exchange, "earn", coin, (existing_count, f"日 {latest_date} 准备同步"))
        else:
            status_update(exchange, "earn", coin, (existing_count, "准备同步"))
        count = append_rows(file_path, rows)
        synced_date = rows[0].get("collect_date", "") if rows else latest_date
        status_update(exchange, "earn", coin, (existing_count + count, f"日 {synced_date} 已完成"))
        log(f"{exchange} {coin} 已写入记录数: {count}")


def mark_missing_coins(exchange: str) -> None:
    """标记交易所未覆盖的链上币种。"""
    supported_coins = set(cex_config.get_earn_coins(exchange))
    for coin in cex_config.CEX_BASE_COINS:
        if coin not in supported_coins:
            status_update(exchange, "earn", coin, cex_config.UNSUPPORTED_STATUS_TEXT)


def mark_unsupported(exchange: str) -> None:
    """标记交易所全部链上状态。"""
    for coin in cex_config.CEX_BASE_COINS:
        status_update(exchange, "earn", coin, cex_config.UNSUPPORTED_STATUS_TEXT)


def main() -> None:
    """运行理财主循环。"""
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
        mark_missing_coins("bybit")
        mark_missing_coins("binance")
        mark_missing_coins("bitget")
        mark_missing_coins("okx")
        sleep_seconds = seconds_until_next_utc_midnight()
        log(f"等待 {sleep_seconds} 秒后再次执行（UTC 00:00）")
        threading.Event().wait(sleep_seconds)


def run() -> None:
    """兼容启动器运行入口。"""
    main()


if __name__ == "__main__":
    run()
