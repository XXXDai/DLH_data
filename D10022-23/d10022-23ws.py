from datetime import datetime, timedelta, timezone
from http.client import IncompleteRead, RemoteDisconnected
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import importlib
import json
import socket
import sys
import threading
import time
import websocket

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

EVENT_URL_TEMPLATES = app_config.POLYMARKET_EVENT_TEMPLATES  # 事件地址模板列表，个数
ASSET_TAGS = app_config.POLYMARKET_ASSET_TAGS  # 资产映射列表，个数
TIMEZONE_NAME = app_config.POLYMARKET_TZ_NAME  # 时区名称，时区
USER_AGENT = "Mozilla/5.0"  # 请求标识，字符串
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"  # WebSocket地址，字符串
RT_DIR = Path("data/src/polymarket_orderbook_rt")  # 原始数据目录，路径
RT_SS_DIR = Path("data/src/polymarket_orderbook_rt_ss")  # 快照数据目录，路径
RT_TAG = "polymarket_orderbook_rt"  # 原始数据标识，字符串
RT_SS_TAG = "polymarket_orderbook_rt_ss"  # 快照数据标识，字符串
HTTP_TIMEOUT_SECONDS = 10  # 事件请求超时，秒
WS_TIMEOUT_SECONDS = 10  # 连接超时，秒
RECV_TIMEOUT_SECONDS = 30  # 接收超时，秒
RECONNECT_INTERVAL_SECONDS = 5  # 重连间隔，秒
STATUS_INTERVAL_SECONDS = 1  # 状态输出间隔，秒
PING_INTERVAL_SECONDS = 10  # 心跳间隔，秒
PRECONNECT_LEAD_SECONDS = 60  # 预连接提前量，秒
BECOME_ACTIVE_AFTER_SECONDS = 90  # 预连接最大等待，秒
SCHEDULER_TICK_SECONDS = 1  # 调度循环间隔，秒
BATCH_SIZE = 2000  # 单文件最大记录数，条
QUIET = bool(globals().get("QUIET", False))  # 静默模式开关，开关
STATUS_HOOK = globals().get("STATUS_HOOK")  # 状态回调函数，函数
LOG_HOOK = globals().get("LOG_HOOK")  # 日志回调函数，函数


def extract_slug(event_url: str) -> str:
    parts = event_url.rstrip("/").split("/event/")
    return parts[1] if len(parts) == 2 else parts[-1]


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def floor_et_epoch(now: datetime, seconds: int) -> int:
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    delta_seconds = int((now - day_start).total_seconds())
    floored = delta_seconds - (delta_seconds % seconds)
    target = day_start + timedelta(seconds=floored)
    return int(target.timestamp())


def build_event_url_at(template: str, name: str, symbol: str, now: datetime) -> str:
    now = now.astimezone(ZoneInfo(TIMEZONE_NAME))
    month = now.strftime("%B").lower()
    day = str(int(now.strftime("%d")))
    hour12 = str(int(now.strftime("%I")))
    ampm = now.strftime("%p").lower()
    context = {
        "name": name,
        "symbol": symbol,
        "month": month,
        "day": day,
        "hour12": hour12,
        "ampm": ampm,
        "epoch_4h": str(floor_et_epoch(now, 4 * 60 * 60)),
        "epoch_15m": str(floor_et_epoch(now, 15 * 60)),
        "epoch_5m": str(floor_et_epoch(now, 5 * 60)),
    }
    return template.format(**context)


def build_event_url(template: str, name: str, symbol: str) -> str:
    now = datetime.now(tz=ZoneInfo(TIMEZONE_NAME))
    return build_event_url_at(template, name, symbol, now)


def template_period_seconds(template: str) -> int:
    if "{epoch_5m}" in template:
        return 5 * 60
    if "{epoch_15m}" in template:
        return 15 * 60
    if "{epoch_4h}" in template:
        return 4 * 60 * 60
    if "{hour12}" in template and "{ampm}" in template:
        return 60 * 60
    return 24 * 60 * 60


def template_label(template: str) -> str:
    if "{epoch_5m}" in template:
        return "5m"
    if "{epoch_15m}" in template:
        return "15m"
    if "{epoch_4h}" in template:
        return "4h"
    if "{hour12}" in template and "{ampm}" in template:
        return "1h"
    return "1d"


def compute_period_bounds_utc(template: str, now_utc: datetime) -> tuple[int, int]:
    tz = ZoneInfo(TIMEZONE_NAME)
    if "{epoch_5m}" in template or "{epoch_15m}" in template or "{epoch_4h}" in template:
        period_seconds = template_period_seconds(template)
        now_local = now_utc.astimezone(tz)
        current_ts = floor_et_epoch(now_local, period_seconds)
        return current_ts, current_ts + period_seconds
    if "{hour12}" in template and "{ampm}" in template:
        now_ts = int(now_utc.timestamp())
        current_ts = (now_ts // 3600) * 3600
        return current_ts, current_ts + 3600
    now_local = now_utc.astimezone(tz)
    today = now_local.date()
    current_local_midnight = datetime(today.year, today.month, today.day, tzinfo=tz)
    tomorrow = today + timedelta(days=1)
    next_local_midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day, tzinfo=tz)
    return int(current_local_midnight.timestamp()), int(next_local_midnight.timestamp())


def parse_asset_tag(text: str) -> tuple[str, str, bool]:
    parts = [item.strip() for item in text.split(":") if item.strip()]
    name = parts[0] if parts else ""
    symbol = parts[1] if len(parts) > 1 else name
    include_5m = parts[2] != "0" if len(parts) > 2 else True
    return name, symbol, include_5m


def fetch_event(slug: str) -> dict:
    url = f"https://gamma-api.polymarket.com/events/slug/{slug}"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=HTTP_TIMEOUT_SECONDS) as response:
        payload = response.read()
    return json.loads(payload)


def extract_asset_ids(event: dict) -> list:
    asset_ids = []
    for market in event.get("markets", []):
        token_ids = market.get("clobTokenIds", [])
        if isinstance(token_ids, str):
            current = []
            for ch in token_ids:
                if "0" <= ch <= "9":
                    current.append(ch)
                    continue
                if current:
                    asset_ids.append("".join(current))
                    current = []
            if current:
                asset_ids.append("".join(current))
            continue
        if isinstance(token_ids, list):
            for token_id in token_ids:
                text = str(token_id)
                if text.isdigit():
                    asset_ids.append(text)
    return asset_ids


def connect_ws() -> websocket.WebSocket:
    ws = websocket.create_connection(WS_URL, timeout=WS_TIMEOUT_SECONDS, origin="https://polymarket.com")
    ws.settimeout(RECV_TIMEOUT_SECONDS)
    return ws


def normalize_asset_ids(asset_ids: list) -> list:
    normalized = []
    for item in asset_ids:
        text = str(item)
        if text.isdigit():
            normalized.append(text)
    return normalized


def send_subscribe(ws: websocket.WebSocket, asset_ids: list) -> None:
    payload = {"type": "market", "assets_ids": normalize_asset_ids(asset_ids)}
    ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def parse_ts_ms(value) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    text = str(value).replace("Z", "+00:00")
    dt = datetime.fromisoformat(text)
    return int(dt.timestamp() * 1000)


def extract_symbol(msg: dict) -> str | None:
    if msg.get("asset_id"):
        return str(msg["asset_id"])
    if msg.get("symbol"):
        return str(msg["symbol"])
    if msg.get("market"):
        return str(msg["market"])
    changes = msg.get("price_changes") or []
    if changes:
        asset_id = changes[0].get("asset_id")
        if asset_id:
            return str(asset_id)
    return None


def iter_payload(msg) -> list:
    if isinstance(msg, list):
        return [item for item in msg if isinstance(item, dict)]
    if isinstance(msg, dict):
        return [msg]
    return []


def build_raw_record(msg: dict, collect_ts: int) -> dict | None:
    if not isinstance(msg, dict):
        return None
    event_type = msg.get("event_type") or msg.get("type")
    if event_type not in {"book", "price_change"}:
        return None
    symbol = extract_symbol(msg)
    if not symbol:
        return None
    ts_ms = parse_ts_ms(msg.get("timestamp") or msg.get("ts"))
    if ts_ms is None:
        return None
    return {
        "topic": None,
        "symbol": symbol,
        "type": event_type,
        "ts": ts_ms,
        "cts": None,
        "collect_ts": collect_ts,
        "data": msg,
    }


def normalize_level(item) -> tuple[str, str] | None:
    if isinstance(item, dict):
        price = item.get("price")
        size = item.get("size")
    elif isinstance(item, (list, tuple)) and len(item) >= 2:
        price, size = item[0], item[1]
    else:
        return None
    if price is None or size is None:
        return None
    return str(price), str(size)


def set_book(side: dict, levels: list) -> None:
    side.clear()
    for item in levels:
        parsed = normalize_level(item)
        if not parsed:
            continue
        price, size = parsed
        if float(size) == 0:
            continue
        side[price] = size


def apply_updates(side: dict, levels: list) -> None:
    for item in levels:
        parsed = normalize_level(item)
        if not parsed:
            continue
        price, size = parsed
        if float(size) == 0:
            side.pop(price, None)
        else:
            side[price] = size


def apply_price_changes(orderbook: dict, changes: list) -> None:
    for change in changes:
        side = str(change.get("side", "")).upper()
        price = change.get("price")
        size = change.get("size")
        if price is None or size is None:
            continue
        price_str = str(price)
        size_str = str(size)
        if side in {"BUY", "BID"}:
            target = orderbook["bids"]
        elif side in {"SELL", "ASK"}:
            target = orderbook["asks"]
        else:
            continue
        if float(size_str) == 0:
            target.pop(price_str, None)
        else:
            target[price_str] = size_str


def update_orderbook(orderbook: dict, msg: dict) -> bool:
    event_type = msg.get("event_type") or msg.get("type")
    if event_type == "book":
        set_book(orderbook["bids"], msg.get("bids", []))
        set_book(orderbook["asks"], msg.get("asks", []))
        return True
    if event_type == "price_change":
        changes = msg.get("price_changes") or []
        if changes:
            apply_price_changes(orderbook, changes)
            return True
        if msg.get("bids") or msg.get("asks"):
            apply_updates(orderbook["bids"], msg.get("bids", []))
            apply_updates(orderbook["asks"], msg.get("asks", []))
            return True
    return False


def build_snapshot(orderbook: dict, msg: dict, collect_ts: int) -> dict | None:
    event_type = msg.get("event_type") or msg.get("type")
    if event_type not in {"book", "price_change"}:
        return None
    symbol = extract_symbol(msg)
    if not symbol:
        return None
    bids_sorted = sorted(orderbook["bids"].items(), key=lambda x: float(x[0]), reverse=True)
    asks_sorted = sorted(orderbook["asks"].items(), key=lambda x: float(x[0]))
    best_bid = float(bids_sorted[0][0]) if bids_sorted else None
    best_ask = float(asks_sorted[0][0]) if asks_sorted else None
    ts_ms = parse_ts_ms(msg.get("timestamp") or msg.get("ts"))
    if ts_ms is None:
        return None
    return {
        "symbol": symbol,
        "update_type": event_type,
        "ts": ts_ms,
        "cts": None,
        "collect_ts": collect_ts,
        "update_id": 0,
        "seq": 0,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": len(bids_sorted),
        "ask_depth": len(asks_sorted),
        "bids": [[price, size] for price, size in bids_sorted],
        "asks": [[price, size] for price, size in asks_sorted],
    }


def build_file_path(base_dir: Path, hour_str: str, tag: str, batch_id: int) -> Path:
    file_name = f"{tag}-{hour_str}-batch_{batch_id:04d}.json"
    return base_dir / hour_str / file_name


def ensure_writer(base_dir: Path, hour_str: str, tag: str, writer):
    if writer and writer["hour"] == hour_str and writer["count"] < BATCH_SIZE:
        return writer
    batch_id = 1
    if writer:
        writer["file"].close()
        if writer["hour"] == hour_str:
            batch_id = writer["batch_id"] + 1
    path = build_file_path(base_dir, hour_str, tag, batch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handle = path.open("a", encoding="utf-8")
    return {"hour": hour_str, "batch_id": batch_id, "count": 0, "file": file_handle}


def hour_str_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d%H")


def close_writer(writer) -> None:
    if writer:
        writer["file"].close()


def load_asset_ids(event_url: str, asset_tag: str, template: str) -> list:
    slug = extract_slug(event_url)
    try:
        event = fetch_event(slug)
    except HTTPError as exc:
        log(f"事件请求失败，HTTP错误: {exc} {event_url}")
        return []
    except URLError as exc:
        log(f"事件请求失败，网络错误: {exc} {event_url}")
        return []
    except RemoteDisconnected as exc:
        log(f"事件请求失败，网络错误: {exc} {event_url}")
        return []
    except IncompleteRead as exc:
        log(f"事件请求失败，网络错误: {exc} {event_url}")
        return []
    except TimeoutError as exc:
        log(f"事件请求失败，超时: {exc} {event_url}")
        return []
    except socket.timeout as exc:
        log(f"事件请求失败，超时: {exc} {event_url}")
        return []
    except json.JSONDecodeError as exc:
        log(f"事件请求失败，返回格式错误: {exc} {event_url}")
        return []
    asset_ids = extract_asset_ids(event)
    if not asset_ids:
        log(f"未获取到可订阅的资产ID: {event_url}")
    return asset_ids


def run_stream(
    asset_ids: list,
    event_url: str,
    status_key: str,
    stop_event: threading.Event,
    active_event: threading.Event,
    ready_event: threading.Event,
) -> None:
    slug = extract_slug(event_url)
    rt_writer = None
    rt_ss_writer = None
    recv_count = 0
    base_count = None
    last_status_ts = time.monotonic()
    orderbooks = {}
    while not stop_event.is_set():
        try:
            ws = connect_ws()
            send_subscribe(ws, asset_ids)
        except websocket.WebSocketException as exc:
            log(f"{status_key} 连接异常，准备重连: {exc} {event_url}")
            time.sleep(RECONNECT_INTERVAL_SECONDS)
            continue
        except TimeoutError as exc:
            log(f"{status_key} 连接超时，准备重连: {exc} {event_url}")
            time.sleep(RECONNECT_INTERVAL_SECONDS)
            continue
        except OSError as exc:
            log(f"{status_key} 网络错误，准备重连: {exc} {event_url}")
            time.sleep(RECONNECT_INTERVAL_SECONDS)
            continue

        def keepalive():
            while not stop_event.is_set():
                time.sleep(PING_INTERVAL_SECONDS)
                try:
                    ws.send("PING")
                except websocket.WebSocketException:
                    break
                except TimeoutError:
                    break
                except OSError:
                    break

        keepalive_thread = threading.Thread(target=keepalive, daemon=True)
        keepalive_thread.start()
        while not stop_event.is_set():
            try:
                raw = ws.recv()
            except websocket.WebSocketException as exc:
                log(f"{status_key} 连接异常，准备重连: {exc} {event_url}")
                break
            except TimeoutError as exc:
                log(f"{status_key} 连接超时，准备重连: {exc} {event_url}")
                break
            except OSError as exc:
                log(f"{status_key} 网络错误，准备重连: {exc} {event_url}")
                break

            recv_count += 1
            now_ts = time.monotonic()
            if now_ts - last_status_ts >= STATUS_INTERVAL_SECONDS:
                if active_event.is_set():
                    if base_count is None:
                        base_count = recv_count
                    if STATUS_HOOK:
                        STATUS_HOOK(status_key, (recv_count - base_count, slug))
                    if not QUIET:
                        print(f"\r已接收数量: {recv_count - base_count}", end="", flush=True)
                last_status_ts = now_ts

            collect_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            if not raw:
                continue
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            if isinstance(raw, str) and raw[:1] not in {"{", "["}:
                continue
            msg = json.loads(raw)
            payloads = iter_payload(msg)
            if payloads and not ready_event.is_set():
                ready_event.set()
            for payload in payloads:
                record = build_raw_record(payload, collect_ts)
                if record:
                    hour_str = hour_str_from_ms(collect_ts)
                    rt_writer = ensure_writer(RT_DIR, hour_str, RT_TAG, rt_writer)
                    rt_writer["file"].write(
                        json.dumps(record, ensure_ascii=True, separators=(",", ":")) + "\n"
                    )
                    rt_writer["count"] += 1
                event_type = payload.get("event_type") or payload.get("type")
                if event_type not in {"book", "price_change"}:
                    continue
                symbol = extract_symbol(payload)
                if not symbol:
                    continue
                orderbook = orderbooks.setdefault(symbol, {"bids": {}, "asks": {}})
                updated = update_orderbook(orderbook, payload)
                if not updated:
                    continue
                snapshot = build_snapshot(orderbook, payload, collect_ts)
                if not snapshot:
                    continue
                hour_str = hour_str_from_ms(collect_ts)
                rt_ss_writer = ensure_writer(RT_SS_DIR, hour_str, RT_SS_TAG, rt_ss_writer)
                rt_ss_writer["file"].write(
                    json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")) + "\n"
                )
                rt_ss_writer["count"] += 1

        try:
            ws.close()
        except websocket.WebSocketException:
            pass
        except TimeoutError:
            pass
        except OSError:
            pass
        time.sleep(RECONNECT_INTERVAL_SECONDS)
    close_writer(rt_writer)
    close_writer(rt_ss_writer)


def run_event_loop(asset_tag: str, template: str, name: str, symbol: str) -> None:
    status_key = f"{symbol.upper()} {template_label(template)}"  # 状态键，字符串
    current_url = None
    current_stop = None
    current_active = None
    current_ready = None
    current_thread = None
    next_url = None
    next_stop = None
    next_active = None
    next_ready = None
    next_thread = None
    next_started_ts = 0

    while True:
        now_utc = datetime.now(tz=timezone.utc)
        now_ts = int(now_utc.timestamp())
        period_start_ts, period_next_ts = compute_period_bounds_utc(template, now_utc)
        tz = ZoneInfo(TIMEZONE_NAME)
        desired_url = build_event_url_at(
            template, name, symbol, datetime.fromtimestamp(period_start_ts, tz=tz)
        )
        desired_next_url = build_event_url_at(
            template, name, symbol, datetime.fromtimestamp(period_next_ts, tz=tz)
        )

        if current_thread and not current_thread.is_alive():
            current_thread = None

        if current_url is None:
            current_url = desired_url

        target_candidate_url = None
        switch_when_ready = False
        if current_url != desired_url:
            target_candidate_url = desired_url
            switch_when_ready = True
        else:
            seconds_to_next = period_next_ts - now_ts
            if seconds_to_next <= PRECONNECT_LEAD_SECONDS or now_ts >= period_next_ts:
                target_candidate_url = desired_next_url
                switch_when_ready = now_ts >= period_next_ts

        if next_thread and not next_thread.is_alive():
            next_thread = None

        if next_url and target_candidate_url != next_url:
            if next_stop:
                next_stop.set()
            next_url = None
            next_stop = None
            next_active = None
            next_ready = None
            next_thread = None
            next_started_ts = 0

        if target_candidate_url and next_url is None:
            asset_ids = load_asset_ids(target_candidate_url, asset_tag, template)
            if asset_ids:
                next_url = target_candidate_url
                next_stop = threading.Event()
                next_active = threading.Event()
                next_ready = threading.Event()
                next_started_ts = now_ts
                next_thread = threading.Thread(
                    target=run_stream,
                    args=(asset_ids, next_url, status_key, next_stop, next_active, next_ready),
                    daemon=True,
                )
                next_thread.start()

        if current_thread is None:
            asset_ids = load_asset_ids(current_url, asset_tag, template)
            if asset_ids:
                current_stop = threading.Event()
                current_active = threading.Event()
                current_ready = threading.Event()
                current_active.set()
                current_thread = threading.Thread(
                    target=run_stream,
                    args=(asset_ids, current_url, status_key, current_stop, current_active, current_ready),
                    daemon=True,
                )
                current_thread.start()

        if next_url and next_ready and next_ready.is_set():
            if switch_when_ready or now_ts >= period_next_ts:
                if next_active:
                    next_active.set()
                if current_active:
                    current_active.clear()
                if current_stop:
                    current_stop.set()
                current_url = next_url
                current_stop = next_stop
                current_active = next_active
                current_ready = next_ready
                current_thread = next_thread
                next_url = None
                next_stop = None
                next_active = None
                next_ready = None
                next_thread = None
                next_started_ts = 0

        if next_url and next_ready and not next_ready.is_set():
            if next_started_ts and now_ts - next_started_ts >= BECOME_ACTIVE_AFTER_SECONDS:
                if next_stop:
                    next_stop.set()
                next_url = None
                next_stop = None
                next_active = None
                next_ready = None
                next_thread = None
                next_started_ts = 0

        time.sleep(SCHEDULER_TICK_SECONDS)


def main() -> None:
    threads = []
    for asset_tag in ASSET_TAGS:
        name, symbol, include_5m = parse_asset_tag(asset_tag)
        if not name or not symbol:
            continue
        for template in EVENT_URL_TEMPLATES:
            if "5m" in template and not include_5m:
                continue
            thread = threading.Thread(target=run_event_loop, args=(asset_tag, template, name, symbol))
            thread.start()
            threads.append(thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


if __name__ == "__main__":
    run()
