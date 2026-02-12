from datetime import datetime, timezone
from pathlib import Path
import json
import importlib
import threading
import time
import sys
import websocket

ROOT_DIR = Path(__file__).resolve().parents[1]  # 项目根目录，路径
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
app_config = importlib.import_module("app_config")  # 项目配置模块，模块

WS_URL = "wss://stream.bybit.com/v5/public/spot"  # WebSocket地址，字符串
SYMBOLS = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)  # 交易对列表，个数
DEPTH = app_config.ORDERBOOK_DEPTH_SPOT  # 订单簿深度，档位
RT_DIR = Path("data/src/bybit_spot_orderbook_rt")  # 原始数据目录，路径
RT_SS_DIR = Path("data/src/bybit_spot_orderbook_rt_ss")  # 快照数据目录，路径
RT_SS_1S_DIR = Path("data/src/bybit_spot_orderbook_rt_ss_1s")  # 秒级快照目录，路径
RT_TAG = "bybit_spot_orderbook_rt"  # 原始数据标识，字符串
RT_SS_TAG = "bybit_spot_orderbook_rt_ss"  # 快照数据标识，字符串
RT_SS_1S_TAG = "bybit_spot_orderbook_rt_ss_1s"  # 秒级快照标识，字符串
TIMEOUT_SECONDS = app_config.WS_TIMEOUT_SECONDS  # 连接超时，秒
RECV_TIMEOUT_SECONDS = app_config.WS_RECV_TIMEOUT_SECONDS  # 接收超时，秒
RECONNECT_INTERVAL_SECONDS = app_config.WS_RECONNECT_INTERVAL_SECONDS  # 重连间隔，秒
STATUS_INTERVAL_SECONDS = app_config.WS_STATUS_INTERVAL_SECONDS  # 状态输出间隔，秒
PING_INTERVAL_SECONDS = app_config.WS_PING_INTERVAL_SECONDS  # 心跳间隔，秒
QUIET = bool(globals().get("QUIET", False))  # 静默模式开关，开关
STATUS_HOOK = globals().get("STATUS_HOOK")  # 状态回调函数，函数
LOG_HOOK = globals().get("LOG_HOOK")  # 日志回调函数，函数


def log(message: str) -> None:
    if LOG_HOOK:
        LOG_HOOK(message)
    if not QUIET:
        print(message)


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


def run_once(symbol: str) -> None:
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
    recv_count = [0]
    last_status_ts = [time.monotonic()]
    orderbook = {"bids": {}, "asks": {}}
    has_snapshot = False
    last_second = None
    last_snapshot = None
    def keepalive():
        while True:
            time.sleep(PING_INTERVAL_SECONDS)
            try:
                ws.send(json.dumps({"op": "ping"}, ensure_ascii=True, separators=(",", ":")))
            except websocket.WebSocketException:
                break
            except TimeoutError:
                break
            except OSError:
                break
            now_ts = time.monotonic()
            if now_ts - last_status_ts[0] >= STATUS_INTERVAL_SECONDS:
                if STATUS_HOOK:
                    STATUS_HOOK(symbol, recv_count[0])
                if not QUIET:
                    print(f"\r已接收数量: {recv_count[0]}", end="", flush=True)
                last_status_ts[0] = now_ts

    keepalive_thread = threading.Thread(target=keepalive, daemon=True)
    keepalive_thread.start()
    while True:
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
        recv_count[0] += 1
        now_ts = time.monotonic()
        if now_ts - last_status_ts[0] >= STATUS_INTERVAL_SECONDS:
            if STATUS_HOOK:
                STATUS_HOOK(symbol, recv_count[0])
            if not QUIET:
                print(f"\r已接收数量: {recv_count[0]}", end="", flush=True)
            last_status_ts[0] = now_ts
        collect_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError as exc:
            log(f"解析失败，跳过: {exc} {symbol}")
            continue
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


def main() -> None:
    if not SYMBOLS:
        log("未配置交易对")
        return
    threads = []
    for symbol in SYMBOLS:
        thread = threading.Thread(target=run_loop, args=(symbol,))
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()


def run() -> None:
    main()


def run_loop(symbol: str) -> None:
    while True:
        run_once(symbol)
        time.sleep(RECONNECT_INTERVAL_SECONDS)


if __name__ == "__main__":
    run()
