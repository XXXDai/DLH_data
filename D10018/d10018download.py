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
ENDPOINT = "/v5/market/insurance"  # 接口路径，字符串
COINS = app_config.BYBIT_INSURANCE_COINS  # 币种列表，个数
TIMEOUT_SECONDS = 10  # 请求超时，秒
DATA_DIR = Path("data/src/bybit_insurance_di")  # 保存目录，路径
QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def build_file_path(base_dir: Path, coin: str) -> Path:
    file_name = f"{coin}_insurance.csv"
    return base_dir / coin / file_name


def request_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("接口请求失败: 网络错误") from exc


def parse_contract_type(symbol: str) -> str:
    return "futures" if "-" in symbol else "perpetual"


def run_coin(coin: str) -> None:
    query = urlencode({"coin": coin})
    url = f"{BASE_URL}{ENDPOINT}?{query}"
    payload = request_json(url)
    if payload.get("retCode") != 0:
        raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
    result = payload.get("result", {})
    items = result.get("list", [])
    now = datetime.now(tz=timezone.utc)
    collect_ts = int(now.timestamp() * 1000)
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    event_ts = int(result.get("updatedTime", "0") or 0)
    event_dt = datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    event_time = datetime.fromtimestamp(event_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for item in items:
        coin = item.get("coin", coin)
        symbols_text = item.get("symbols", "")
        symbols = [s for s in symbols_text.split(",") if s]
        if not symbols:
            continue
        pool_symbol_num = len(symbols)
        pool_type = "shared" if pool_symbol_num > 1 else "dedicated"
        for symbol in symbols:
            rows.append(
                {
                    "symbol": symbol,
                    "pool_type": pool_type,
                    "contract_type": parse_contract_type(symbol),
                    "pool_symbol_list": symbols_text,
                    "pool_symbol_num": str(pool_symbol_num),
                    "pool_balance": item.get("balance", ""),
                    "pool_value": item.get("value", ""),
                    "coin": coin,
                    "event_ts": str(event_ts),
                    "event_dt": event_dt,
                    "event_time": event_time,
                    "collect_ts": str(collect_ts),
                    "collect_date": collect_date,
                    "collect_time": collect_time,
                }
            )
    file_path = build_file_path(DATA_DIR, coin)
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
    log(f"已写入记录数: {len(rows)}")


def main() -> None:
    threads = []
    for coin in COINS:
        thread = threading.Thread(target=run_coin, args=(coin,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


if __name__ == "__main__":
    run()
