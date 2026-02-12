from datetime import datetime, timezone
from pathlib import Path
import csv
import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import app_config

BASE_URL = "https://api.bybit.com"  # API根地址，字符串
ENDPOINT = "/v5/earn/product"  # 接口路径，字符串
CATEGORY = "OnChain"  # 产品类型，字符串
TIMEOUT_SECONDS = 10  # 请求超时，秒
DATA_DIR = Path("data/src/bybit_onchainstaking_di")  # 保存目录，路径
FILTER_COINS = app_config.BYBIT_ONCHAIN_COINS  # 币种过滤列表，个数
QUIET = False  # 静默模式开关，开关


def log(message: str) -> None:
    if not QUIET:
        print(message)


def build_file_path(base_dir: Path, coin: str) -> Path:
    file_name = f"{coin}_onchainstaking.csv"
    return base_dir / coin / file_name


def request_json(url: str) -> dict:
    try:
        with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("接口请求失败: 网络错误") from exc


def normalize_apr(apr_text: str) -> str:
    text = (apr_text or "").strip()
    if not text:
        return ""
    if text.endswith("%"):
        value = float(text.rstrip("%")) / 100
        formatted = f"{value:.6f}".rstrip("0").rstrip(".")
        return formatted
    return text


def main() -> None:
    query = urlencode({"category": CATEGORY})
    url = f"{BASE_URL}{ENDPOINT}?{query}"
    payload = request_json(url)
    if payload.get("retCode") != 0:
        raise RuntimeError(f"接口返回错误: {payload.get('retMsg')}")
    items = payload.get("result", {}).get("list", [])
    now = datetime.now(tz=timezone.utc)
    collect_ts = int(now.timestamp() * 1000)
    collect_date = now.strftime("%Y-%m-%d")
    collect_time = now.strftime("%Y-%m-%d %H:%M:%S")
    rows_by_coin = {}
    for item in items:
        coin = item.get("coin", "")
        if not coin:
            continue
        if FILTER_COINS and coin not in FILTER_COINS:
            continue
        row = {
            "coin": coin,
            "lst": item.get("swapCoin", ""),
            "apr": normalize_apr(item.get("estimateApr", "")),
            "product_id": item.get("productId", ""),
            "stake_rate": item.get("stakeExchangeRate", ""),
            "redeem_rate": item.get("redeemExchangeRate", ""),
            "min_stake": item.get("minStakeAmount", ""),
            "max_stake": item.get("maxStakeAmount", ""),
            "collect_ts": str(collect_ts),
            "collect_date": collect_date,
            "collect_time": collect_time,
        }
        rows_by_coin.setdefault(coin, []).append(row)
    total = 0
    for coin, rows in rows_by_coin.items():
        file_path = build_file_path(DATA_DIR, coin)
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
        total += len(rows)
    log(f"已写入记录数: {total}")


def run() -> None:
    main()


if __name__ == "__main__":
    run()
