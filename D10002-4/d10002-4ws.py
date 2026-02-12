from datetime import datetime, timezone
from pathlib import Path
import re
import json
import importlib
import threading
import time
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen
import websocket

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

WS_URL = "wss://stream.bybit.com/v5/public/linear"  # WebSocket地址，字符串
INSTRUMENTS_BASE_URL = "https://api.bybit.com/v5/market/instruments-info"  # 交易对接口地址，字符串
INSTRUMENTS_TIMEOUT_SECONDS = 10  # 交易对接口超时，秒
INSTRUMENTS_LIMIT = 1000  # 交易对接口分页大小，条
DELIVERY_CATEGORIES = app_config.BYBIT_FUTURE_DELIVERY_CATEGORIES  # 交割合约产品类型列表，个数
DELIVERY_STATUSES = app_config.BYBIT_FUTURE_DELIVERY_STATUSES  # 交割合约状态列表，个数
DELIVERY_EXCLUDE = app_config.BYBIT_FUTURE_DELIVERY_EXCLUDE  # 交割合约过滤列表，个数
DELIVERY_SYMBOL_PATTERN = re.compile(r".+-\\d{2}[A-Z]{3}\\d{2}$")  # 交割合约格式，正则
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
DEPTH = 200  # 订单簿深度，档位
RT_DIR = Path("data/src/bybit_future_orderbook_rt")  # 原始数据目录，路径
RT_SS_DIR = Path("data/src/bybit_future_orderbook_rt_ss")  # 快照数据目录，路径
RT_SS_1S_DIR = Path("data/src/bybit_future_orderbook_rt_ss_1s")  # 秒级快照目录，路径
RT_TAG = "bybit_future_orderbook_rt"  # 原始数据标识，字符串
RT_SS_TAG = "bybit_future_orderbook_rt_ss"  # 快照数据标识，字符串
RT_SS_1S_TAG = "bybit_future_orderbook_rt_ss_1s"  # 秒级快照标识，字符串
TIMEOUT_SECONDS = 10  # 连接超时，秒
RECV_TIMEOUT_SECONDS = 30  # 接收超时，秒
RECONNECT_INTERVAL_SECONDS = 5  # 重连间隔，秒
STATUS_INTERVAL_SECONDS = 1  # 状态输出间隔，秒
DELIVERY_REFRESH_SECONDS = 15 * 60  # 交割合约刷新间隔，秒
QUIET = bool(globals().get("QUIET", False))  # 静默模式开关，开关
STATUS_HOOK = globals().get("STATUS_HOOK")  # 状态回调函数，函数
LOG_HOOK = globals().get("LOG_HOOK")  # 日志回调函数，函数
THREAD_LOCK = threading.Lock()  # 线程锁，锁


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


def request_json(url: str) -> dict:
    with urlopen(url, timeout=INSTRUMENTS_TIMEOUT_SECONDS) as response:
        return json.loads(response.read().decode("utf-8"))


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
                    if delivery_time < start_ms:
                        continue
                    symbol = item.get("symbol")
                    if symbol and not DELIVERY_SYMBOL_PATTERN.match(symbol):
                        continue
                    if symbol and not symbol.endswith("USDT"):
                        continue
                    if symbol and symbol not in DELIVERY_EXCLUDE:
                        symbols[symbol] = delivery_time
                cursor = result.get("nextPageCursor")
                if not cursor:
                    break
    return symbols


def build_active_symbols() -> set:
    start_date = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    delivery_map = list_delivery_symbols(start_date)
    active_delivery = {symbol for symbol, ts in delivery_map.items() if ts >= now_ms}
    return set(SYMBOLS) | active_delivery


def build_topic(symbol: str, depth: int) -> str:
    return f"orderbook.{depth}.{symbol}"


def build_file_path(base_dir: Path, symbol: str, hour_str: str, tag: str) -> Path:
    file_name = f"{symbol}-{tag}-{hour_str}.json"
    return base_dir / symbol / hour_str / file_name


def connect_ws() -> websocket.WebSocket:
    ws = websocket.create_connection(WS_URL, timeout=TIMEOUT_SECONDS)
    ws.settimeout(RECV_TIMEOUT_SECONDS)
    return ws


def send_subscribe(ws: websocket.WebSocket, topic: str) -> None:
    payload = {"op": "subscribe", "args": [topic]}
    ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def ensure_writer(base_dir: Path, symbol: str, hour_str: str, tag: str, writer):
    if writer and writer[0] == hour_str:
        return writer
    if writer:
        writer[1].close()
    path = build_file_path(base_dir, symbol, hour_str, tag)
    path.parent.mkdir(parents=True, exist_ok=True)
    file_handle = path.open("a", encoding="utf-8")
    return hour_str, file_handle


def normalize_message(msg: dict, collect_ts: int, fallback_symbol: str) -> dict:
    if "symbol" not in msg:
        data = msg.get("data", {})
        msg["symbol"] = data.get("s", fallback_symbol)
    msg["collect_ts"] = collect_ts
    return msg


def update_orderbook(orderbook: dict, msg_type: str, data: dict) -> bool:
    if msg_type == "snapshot":
        orderbook["bids"] = {float(b[0]): float(b[1]) for b in data.get("b", [])}
        orderbook["asks"] = {float(a[0]): float(a[1]) for a in data.get("a", [])}
        return True
    if msg_type == "delta":
        for price, size in data.get("b", []):
            price, size = float(price), float(size)
            if size == 0:
                orderbook["bids"].pop(price, None)
            else:
                orderbook["bids"][price] = size
        for price, size in data.get("a", []):
            price, size = float(price), float(size)
            if size == 0:
                orderbook["asks"].pop(price, None)
            else:
                orderbook["asks"][price] = size
        return True
    return False


def format_number(value: float) -> str:
    text = f"{value:.10f}".rstrip("0").rstrip(".")
    return text if text else "0"


def build_snapshot(orderbook: dict, msg: dict, data: dict, fallback_symbol: str) -> dict:
    bids_sorted = sorted(orderbook["bids"].items(), key=lambda x: x[0], reverse=True)
    asks_sorted = sorted(orderbook["asks"].items(), key=lambda x: x[0])
    best_bid = bids_sorted[0][0] if bids_sorted else None
    best_ask = asks_sorted[0][0] if asks_sorted else None
    return {
        "symbol": data.get("s", msg.get("symbol", fallback_symbol)),
        "update_type": msg.get("type"),
        "ts": msg.get("ts"),
        "cts": msg.get("cts"),
        "collect_ts": msg.get("collect_ts"),
        "update_id": data.get("u"),
        "seq": data.get("seq"),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": len(bids_sorted),
        "ask_depth": len(asks_sorted),
        "bids": [[format_number(p), format_number(s)] for p, s in bids_sorted],
        "asks": [[format_number(p), format_number(s)] for p, s in asks_sorted],
    }


def hour_str_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d%H")


def close_writer(writer) -> None:
    if writer:
        writer[1].close()


def run_once(symbol: str, stop_event: threading.Event) -> None:
    if stop_event.is_set():
        return
    topic = build_topic(symbol, DEPTH)
    try:
        ws = connect_ws()
        send_subscribe(ws, topic)
    except websocket.WebSocketException as exc:
        log(f"连接异常，准备重连: {exc} {symbol}")
        return
    except TimeoutError as exc:
        log(f"连接超时，准备重连: {exc} {symbol}")
        return
    except OSError as exc:
        log(f"网络错误，准备重连: {exc} {symbol}")
        return
    rt_writer = None
    rt_ss_writer = None
    rt_ss_1s_writer = None
    recv_count = 0
    last_status_ts = time.monotonic()
    orderbook = {"bids": {}, "asks": {}}
    has_snapshot = False
    last_second = None
    last_snapshot = None
    while True:
        if stop_event.is_set():
            ws.close()
            break
        try:
            raw = ws.recv()
        except websocket.WebSocketException as exc:
            log(f"连接异常，准备重连: {exc} {symbol}")
            break
        except TimeoutError as exc:
            log(f"连接超时，准备重连: {exc} {symbol}")
            break
        except OSError as exc:
            log(f"网络错误，准备重连: {exc} {symbol}")
            break
        recv_count += 1
        now_ts = time.monotonic()
        if now_ts - last_status_ts >= STATUS_INTERVAL_SECONDS:
            if STATUS_HOOK:
                STATUS_HOOK(symbol, recv_count)
            if not QUIET:
                print(f"\r已接收数量: {recv_count}", end="", flush=True)
            last_status_ts = now_ts
        collect_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        msg = json.loads(raw)
        if "topic" not in msg:
            continue
        msg = normalize_message(msg, collect_ts, symbol)
        hour_str = datetime.now(tz=timezone.utc).strftime("%Y%m%d%H")
        rt_writer = ensure_writer(RT_DIR, symbol, hour_str, RT_TAG, rt_writer)
        rt_writer[1].write(json.dumps(msg, ensure_ascii=True, separators=(",", ":")) + "\n")
        data = msg.get("data", {})
        msg_type = msg.get("type")
        if msg_type == "snapshot":
            has_snapshot = update_orderbook(orderbook, msg_type, data)
        elif msg_type == "delta" and has_snapshot:
            update_orderbook(orderbook, msg_type, data)
        else:
            continue
        snapshot = build_snapshot(orderbook, msg, data, symbol)
        rt_ss_writer = ensure_writer(RT_SS_DIR, symbol, hour_str, RT_SS_TAG, rt_ss_writer)
        rt_ss_writer[1].write(json.dumps(snapshot, ensure_ascii=True, separators=(",", ":")) + "\n")
        second_bucket = int(collect_ts / 1000)
        if last_second is None:
            last_second = second_bucket
        if second_bucket != last_second and last_snapshot:
            snapshot_hour = hour_str_from_ms(last_snapshot["collect_ts"])
            rt_ss_1s_writer = ensure_writer(
                RT_SS_1S_DIR, symbol, snapshot_hour, RT_SS_1S_TAG, rt_ss_1s_writer
            )
            rt_ss_1s_writer[1].write(
                json.dumps(last_snapshot, ensure_ascii=True, separators=(",", ":")) + "\n"
            )
            last_second = second_bucket
        last_snapshot = snapshot
    if last_snapshot:
        snapshot_hour = hour_str_from_ms(last_snapshot["collect_ts"])
        rt_ss_1s_writer = ensure_writer(
            RT_SS_1S_DIR, symbol, snapshot_hour, RT_SS_1S_TAG, rt_ss_1s_writer
        )
        rt_ss_1s_writer[1].write(
            json.dumps(last_snapshot, ensure_ascii=True, separators=(",", ":")) + "\n"
        )
    close_writer(rt_writer)
    close_writer(rt_ss_writer)
    close_writer(rt_ss_1s_writer)


def start_symbol_thread(symbol: str, thread_map: dict) -> None:
    stop_event = threading.Event()
    thread = threading.Thread(target=run_loop, args=(symbol, stop_event))
    thread.start()
    thread_map[symbol] = (thread, stop_event)


def refresh_loop(thread_map: dict) -> None:
    while True:
        try:
            desired = build_active_symbols()
        except HTTPError as exc:
            log(f"交割合约获取失败，HTTP错误: {exc}")
            time.sleep(DELIVERY_REFRESH_SECONDS)
            continue
        except URLError as exc:
            log(f"交割合约获取失败，网络错误: {exc}")
            time.sleep(DELIVERY_REFRESH_SECONDS)
            continue
        with THREAD_LOCK:
            current = set(thread_map.keys())
            to_stop = current - desired
            to_start = desired - current
            for symbol in to_stop:
                thread, stop_event = thread_map.pop(symbol)
                stop_event.set()
            for symbol in sorted(to_start):
                start_symbol_thread(symbol, thread_map)
        time.sleep(DELIVERY_REFRESH_SECONDS)


def main() -> None:
    thread_map = {}
    try:
        desired = build_active_symbols()
    except HTTPError as exc:
        log(f"交割合约获取失败，HTTP错误: {exc}")
        desired = set(SYMBOLS)
    except URLError as exc:
        log(f"交割合约获取失败，网络错误: {exc}")
        desired = set(SYMBOLS)
    if not desired:
        log("未配置交易对")
        return
    with THREAD_LOCK:
        for symbol in sorted(desired):
            start_symbol_thread(symbol, thread_map)
    refresh_thread = threading.Thread(target=refresh_loop, args=(thread_map,))
    refresh_thread.start()
    refresh_thread.join()


def run() -> None:
    main()


def run_loop(symbol: str, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        run_once(symbol, stop_event)
        if stop_event.is_set():
            break
        time.sleep(RECONNECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
