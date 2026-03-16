from collections import deque
from datetime import datetime, timezone
from pathlib import Path
import json
import socket
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import websocket

import app_config
from cex import cex_config
from cex.cex_common import upload_file_to_s3


BYBIT_FUTURE_WS_URL = "wss://stream.bybit.com/v5/public/linear"  # Bybit期货WS地址，字符串
BYBIT_SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"  # Bybit现货WS地址，字符串
BYBIT_INSTRUMENTS_URL = "https://api.bybit.com/v5/market/instruments-info"  # Bybit合约接口地址，字符串
BINANCE_FUTURE_WS_URL = "wss://fstream.binance.com/ws/{stream}"  # Binance期货WS地址模板，字符串
BINANCE_SPOT_WS_URL = "wss://stream.binance.com:9443/ws/{stream}"  # Binance现货WS地址模板，字符串
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"  # Bitget公共WS地址，字符串
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"  # OKX公共WS地址，字符串
OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"  # OKX合约接口地址，字符串
INSTRUMENTS_TIMEOUT_SECONDS = app_config.INSTRUMENTS_TIMEOUT_SECONDS  # 合约接口超时，秒
INSTRUMENTS_LIMIT = app_config.INSTRUMENTS_LIMIT  # Bybit合约接口分页大小，条
KEEP_ORDERBOOK_LEVELS = 2000  # 内存保留盘口层数，档位
TIMEOUT_SECONDS = app_config.WS_TIMEOUT_SECONDS  # WS连接超时，秒
RECV_TIMEOUT_SECONDS = app_config.WS_RECV_TIMEOUT_SECONDS  # WS接收超时，秒
RECONNECT_INTERVAL_SECONDS = app_config.WS_RECONNECT_INTERVAL_SECONDS  # WS重连间隔，秒
STATUS_INTERVAL_SECONDS = app_config.WS_STATUS_INTERVAL_SECONDS  # WS状态刷新间隔，秒
PING_INTERVAL_SECONDS = app_config.WS_PING_INTERVAL_SECONDS  # WS心跳间隔，秒
DELIVERY_REFRESH_SECONDS = app_config.DELIVERY_REFRESH_SECONDS  # 动态合约刷新间隔，秒
QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数
MARKET_QUIET = {"future": False, "spot": False}  # 分市场静默开关映射，映射
MARKET_STATUS_HOOK = {"future": None, "spot": None}  # 分市场状态回调映射，映射
MARKET_LOG_HOOK = {"future": None, "spot": None}  # 分市场日志回调映射，映射
PRIMARY_ROLE = "primary"  # 主连接角色标识，字符串
BACKUP_ROLE = "backup"  # 备连接角色标识，字符串


class NetworkRequestError(RuntimeError):
    """表示网络请求失败。"""


def configure_market_runtime(market: str, quiet: bool, status_hook, log_hook) -> None:
    """配置指定市场的运行时回调。"""
    MARKET_QUIET[market] = quiet
    MARKET_STATUS_HOOK[market] = status_hook
    MARKET_LOG_HOOK[market] = log_hook


def log(message: str, market: str | None = None) -> None:
    """输出日志消息。"""
    hook = MARKET_LOG_HOOK.get(market) if market else LOG_HOOK
    quiet = MARKET_QUIET.get(market, QUIET) if market else QUIET
    if hook:
        hook(message)
    if not quiet:
        print(message)


def status_update(exchange: str, market: str, symbol: str, value) -> None:
    """更新统一状态键的状态值。"""
    hook = MARKET_STATUS_HOOK.get(market) or STATUS_HOOK
    if hook:
        hook(cex_config.get_status_key(exchange, market, symbol), value)


def request_json(url: str) -> dict:
    """请求JSON接口并返回字典。"""
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=INSTRUMENTS_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise NetworkRequestError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise NetworkRequestError("接口请求失败: 网络错误") from exc
    except TimeoutError as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc
    except socket.timeout as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc


def build_dirs(exchange: str, market: str) -> tuple[Path, Path, Path, str, str, str]:
    """构造目录与文件标签。"""
    return (
        cex_config.build_standard_orderbook_rt_dir(exchange, market, "rt"),
        cex_config.build_standard_orderbook_rt_dir(exchange, market, "rt_ss"),
        cex_config.build_standard_orderbook_rt_dir(exchange, market, "rt_ss_1s"),
        cex_config.build_standard_orderbook_rt_tag(exchange, market, "rt"),
        cex_config.build_standard_orderbook_rt_tag(exchange, market, "rt_ss"),
        cex_config.build_standard_orderbook_rt_tag(exchange, market, "rt_ss_1s"),
    )


def build_file_path(base_dir: Path, symbol: str, hour_str: str, tag: str) -> Path:
    """构造单小时JSON文件路径。"""
    file_name = f"{symbol}-{tag}-{hour_str}.json"
    return base_dir / symbol / hour_str / file_name


def ensure_writer(base_dir: Path, symbol: str, hour_str: str, tag: str, writer):
    """按小时切换输出文件句柄。"""
    if writer and writer[0] == hour_str:
        return writer
    if writer:
        writer[2].close()
        upload_file_to_s3(writer[1])
    path = build_file_path(base_dir, symbol, hour_str, tag)
    path.parent.mkdir(parents=True, exist_ok=True)
    return hour_str, path, path.open("a", encoding="utf-8")


def close_writer(writer) -> None:
    """关闭输出文件句柄。"""
    if writer:
        writer[2].close()
        upload_file_to_s3(writer[1])


def hour_str_from_ms(ts_ms: int) -> str:
    """将毫秒时间戳格式化为小时分桶。"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d%H")


def format_number(value: float) -> str:
    """格式化盘口价格与数量。"""
    text = f"{value:.10f}".rstrip("0").rstrip(".")
    return text if text else "0"


def replace_orderbook(orderbook: dict, bids: list, asks: list) -> None:
    """用快照整体替换内存盘口。"""
    orderbook["bids"] = {float(price): float(size) for price, size in bids}
    orderbook["asks"] = {float(price): float(size) for price, size in asks}


def apply_orderbook_delta(orderbook: dict, bids: list, asks: list) -> None:
    """将增量更新应用到内存盘口。"""
    for price_text, size_text in bids:
        price = float(price_text)
        size = float(size_text)
        if size == 0:
            orderbook["bids"].pop(price, None)
        else:
            orderbook["bids"][price] = size
    for price_text, size_text in asks:
        price = float(price_text)
        size = float(size_text)
        if size == 0:
            orderbook["asks"].pop(price, None)
        else:
            orderbook["asks"][price] = size


def trim_orderbook(orderbook: dict) -> None:
    """裁剪内存盘口，避免长期累积过深。"""
    if len(orderbook["bids"]) > KEEP_ORDERBOOK_LEVELS:
        keep_prices = sorted(orderbook["bids"], reverse=True)[:KEEP_ORDERBOOK_LEVELS]
        orderbook["bids"] = {price: orderbook["bids"][price] for price in keep_prices}
    if len(orderbook["asks"]) > KEEP_ORDERBOOK_LEVELS:
        keep_prices = sorted(orderbook["asks"])[:KEEP_ORDERBOOK_LEVELS]
        orderbook["asks"] = {price: orderbook["asks"][price] for price in keep_prices}


def build_snapshot(
    exchange: str,
    market: str,
    symbol: str,
    orderbook: dict,
    update_type: str,
    ts_ms: int,
    cts_ms: int,
    collect_ts: int,
    update_id: int | None,
    seq: int | None,
    depth: int,
) -> dict:
    """构造统一快照结构。"""
    bids_sorted = sorted(orderbook["bids"].items(), key=lambda item: item[0], reverse=True)[:depth]
    asks_sorted = sorted(orderbook["asks"].items(), key=lambda item: item[0])[:depth]
    best_bid = bids_sorted[0][0] if bids_sorted else None
    best_ask = asks_sorted[0][0] if asks_sorted else None
    return {
        "symbol": symbol,
        "update_type": update_type,
        "ts": ts_ms,
        "cts": cts_ms,
        "collect_ts": collect_ts,
        "update_id": update_id,
        "seq": seq,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": len(bids_sorted),
        "ask_depth": len(asks_sorted),
        "bids": [[format_number(price), format_number(size)] for price, size in bids_sorted],
        "asks": [[format_number(price), format_number(size)] for price, size in asks_sorted],
    }


def build_bybit_instruments_url(status: str, cursor: str | None) -> str:
    """构造Bybit合约列表接口。"""
    params = {
        "category": "linear",
        "status": status,
        "limit": INSTRUMENTS_LIMIT,
    }
    if cursor:
        params["cursor"] = cursor
    return f"{BYBIT_INSTRUMENTS_URL}?{urlencode(params)}"


def list_bybit_delivery_symbol_times(start_date: str) -> dict[str, int]:
    """拉取Bybit指定起始日之后的交割合约时间映射。"""
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    symbols = set()
    symbol_times = {}
    for status in cex_config.BYBIT_FUTURE_DELIVERY_STATUSES:
        cursor = None
        while True:
            payload = request_json(build_bybit_instruments_url(status, cursor))
            if payload.get("retCode") != 0:
                raise NetworkRequestError(f"接口返回错误: {payload.get('retMsg')}")
            result = payload.get("result", {})
            for item in result.get("list", []):
                symbol = item.get("symbol", "")
                contract_type = item.get("contractType", "")
                delivery_time = int(item.get("deliveryTime", "0") or 0)
                if "Futures" not in contract_type:
                    continue
                if not delivery_time or delivery_time < start_ms:
                    continue
                base_symbol = symbol.split("-")[0] if symbol else ""
                if base_symbol in cex_config.BYBIT_FUTURE_DELIVERY_EXCLUDE:
                    continue
                if base_symbol in cex_config.get_delivery_families("bybit"):
                    symbols.add(symbol)
                    symbol_times[symbol] = delivery_time
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
    return {symbol: symbol_times[symbol] for symbol in sorted(symbols)}


def list_bybit_delivery_symbol_start_dates(start_date: str) -> dict[str, str]:
    """拉取Bybit指定起始日之后的交割合约起始日期映射。"""
    start_ms = int(datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp() * 1000)
    symbol_dates = {}
    for status in cex_config.BYBIT_FUTURE_DELIVERY_STATUSES:
        cursor = None
        while True:
            payload = request_json(build_bybit_instruments_url(status, cursor))
            if payload.get("retCode") != 0:
                raise NetworkRequestError(f"接口返回错误: {payload.get('retMsg')}")
            result = payload.get("result", {})
            for item in result.get("list", []):
                symbol = item.get("symbol", "")
                contract_type = item.get("contractType", "")
                launch_time = int(item.get("launchTime", "0") or 0)
                if "Futures" not in contract_type:
                    continue
                if not launch_time or launch_time < start_ms:
                    continue
                base_symbol = symbol.split("-")[0] if symbol else ""
                if base_symbol in cex_config.BYBIT_FUTURE_DELIVERY_EXCLUDE:
                    continue
                if base_symbol in cex_config.get_delivery_families("bybit"):
                    symbol_dates[symbol] = datetime.fromtimestamp(launch_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            cursor = result.get("nextPageCursor")
            if not cursor:
                break
    return {symbol: symbol_dates[symbol] for symbol in sorted(symbol_dates)}


def list_bybit_delivery_symbols_since(start_date: str) -> list[str]:
    """拉取Bybit指定起始日之后的交割合约。"""
    return sorted(list_bybit_delivery_symbol_times(start_date).keys())


def list_bybit_delivery_symbols() -> list[str]:
    """拉取Bybit当前可订阅的交割合约。"""
    now = datetime.now(tz=timezone.utc)
    symbol_times = list_bybit_delivery_symbol_times(now.strftime("%Y-%m-%d"))
    now_ms = int(now.timestamp() * 1000)
    return sorted([symbol for symbol, delivery_time in symbol_times.items() if delivery_time >= now_ms])


def list_okx_delivery_symbols() -> list[str]:
    """拉取OKX当前可订阅的交割合约。"""
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    symbols = set()
    for family in cex_config.get_delivery_families("okx"):
        try:
            payload = request_json(f"{OKX_INSTRUMENTS_URL}?{urlencode({'instType': 'FUTURES', 'instFamily': family})}")
        except NetworkRequestError:
            continue
        if payload.get("code") != "0":
            raise NetworkRequestError(f"接口返回错误: {payload.get('msg')}")
        for item in payload.get("data", []):
            inst_id = item.get("instId", "")
            state = item.get("state", "")
            exp_time = int(item.get("expTime", "0") or 0)
            if state not in {"live", "preopen"}:
                continue
            if exp_time and exp_time < now_ms:
                continue
            symbols.add(inst_id)
    return sorted(symbols)


def list_okx_delivery_symbol_start_dates() -> dict[str, str]:
    """拉取OKX当前可订阅交割合约的起始日期映射。"""
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    symbol_dates = {}
    for family in cex_config.get_delivery_families("okx"):
        try:
            payload = request_json(f"{OKX_INSTRUMENTS_URL}?{urlencode({'instType': 'FUTURES', 'instFamily': family})}")
        except NetworkRequestError:
            continue
        if payload.get("code") != "0":
            raise NetworkRequestError(f"接口返回错误: {payload.get('msg')}")
        for item in payload.get("data", []):
            inst_id = item.get("instId", "")
            state = item.get("state", "")
            exp_time = int(item.get("expTime", "0") or 0)
            list_time = int(item.get("listTime", "0") or 0)
            if state not in {"live", "preopen"}:
                continue
            if exp_time and exp_time < now_ms:
                continue
            if not list_time:
                continue
            symbol_dates[inst_id] = datetime.fromtimestamp(list_time / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
    return {symbol: symbol_dates[symbol] for symbol in sorted(symbol_dates)}


def resolve_symbols(exchange: str, market: str) -> list[str]:
    """解析某个交易所当前应运行的交易对。"""
    if market == "spot":
        return cex_config.get_spot_symbols(exchange)
    symbols = set(cex_config.get_future_symbols(exchange))
    if exchange == "bybit":
        symbols.update(list_bybit_delivery_symbols())
    if exchange == "okx":
        symbols.update(list_okx_delivery_symbols())
    return sorted(symbols)


def connect_ws(url: str) -> websocket.WebSocket:
    """建立WebSocket连接。"""
    ws = websocket.create_connection(url, timeout=TIMEOUT_SECONDS)
    ws.settimeout(RECV_TIMEOUT_SECONDS)
    return ws


def build_ws_url(exchange: str, market: str, symbol: str) -> str:
    """构造指定交易所的WebSocket地址。"""
    if exchange == "bybit":
        return BYBIT_FUTURE_WS_URL if market == "future" else BYBIT_SPOT_WS_URL
    if exchange == "binance":
        stream = f"{symbol.lower()}@depth20@100ms"
        return (BINANCE_FUTURE_WS_URL if market == "future" else BINANCE_SPOT_WS_URL).format(stream=stream)
    if exchange == "bitget":
        return BITGET_WS_URL
    if exchange == "okx":
        return OKX_WS_URL
    raise RuntimeError(f"未支持的交易所: {exchange}")


def build_ws_urls(exchange: str, market: str, symbol: str) -> list[str]:
    """构造主备WebSocket地址列表。"""
    primary_url = build_ws_url(exchange, market, symbol)
    return [primary_url, primary_url]


def build_session_state() -> dict:
    """构造主备连接共享状态。"""
    return {
        "lock": threading.Lock(),
        "active_role": PRIMARY_ROLE,
        "connected": {PRIMARY_ROLE: False, BACKUP_ROLE: False},
        "recv_count": {PRIMARY_ROLE: 0, BACKUP_ROLE: 0},
        "status_text": {PRIMARY_ROLE: "准备连接", BACKUP_ROLE: "准备连接"},
    }


def role_label(role: str) -> str:
    """返回连接角色显示名称。"""
    return "主" if role == PRIMARY_ROLE else "备"


def other_role(role: str) -> str:
    """返回另一个连接角色。"""
    return BACKUP_ROLE if role == PRIMARY_ROLE else PRIMARY_ROLE


def summarize_role_status(connected: bool, status_text: str) -> str:
    """将单个角色状态压缩为短文本。"""
    if connected:
        return "在线"
    if status_text in {"准备连接", "已连接 0"}:
        return "连接中"
    if status_text in {"连接异常", "连接超时", "网络错误", "连接关闭"}:
        return "重连中"
    if status_text == "已停止":
        return "已停"
    return status_text


def build_ws_status_text(state: dict) -> tuple[int, str]:
    """构造主备连接汇总状态文本。"""
    active_role = state["active_role"]
    active_count = state["recv_count"][active_role]
    primary_connected = state["connected"][PRIMARY_ROLE]
    backup_connected = state["connected"][BACKUP_ROLE]
    primary_short = summarize_role_status(primary_connected, state["status_text"][PRIMARY_ROLE])
    backup_short = summarize_role_status(backup_connected, state["status_text"][BACKUP_ROLE])
    if primary_connected and backup_connected:
        role_summary = "主活 备连" if active_role == PRIMARY_ROLE else "备活 主连"
    elif primary_connected:
        role_summary = "主活 备断" if active_role == PRIMARY_ROLE else "已切主"
    elif backup_connected:
        role_summary = "已切备" if active_role == BACKUP_ROLE else "备活 主断"
    else:
        role_summary = "双断重连中"
    online_flag = 1 if state["connected"][active_role] else 0
    return online_flag, f"{role_summary} | 主:{primary_short} 备:{backup_short} | 消息:{active_count}"


def is_active_role(state: dict, role: str) -> bool:
    """判断指定角色是否为当前主用连接。"""
    with state["lock"]:
        return state["active_role"] == role


def update_shared_status(
    state: dict,
    exchange: str,
    market: str,
    symbol: str,
    role: str,
    *,
    connected: bool | None = None,
    status_text: str | None = None,
    recv_count: int | None = None,
) -> None:
    """更新主备共享状态并刷新统一状态文本。"""
    with state["lock"]:
        if connected is not None:
            state["connected"][role] = connected
        if status_text is not None:
            state["status_text"][role] = status_text
        if recv_count is not None:
            state["recv_count"][role] = recv_count
        online_flag, status_text_value = build_ws_status_text(state)
    status_update(exchange, market, symbol, (online_flag, status_text_value))


def switch_active_role(state: dict, exchange: str, market: str, symbol: str, failed_role: str) -> None:
    """在主连接失效时切换到另一个角色。"""
    next_role = other_role(failed_role)
    switched = False
    with state["lock"]:
        if state["active_role"] == failed_role and state["connected"][next_role]:
            state["active_role"] = next_role
            switched = True
    if switched:
        log(f"{exchange} {market} {symbol} 主备切换到{role_label(next_role)}连接", market)
    update_shared_status(state, exchange, market, symbol, failed_role)


def send_subscribe(ws: websocket.WebSocket, exchange: str, market: str, symbol: str, depth: int) -> None:
    """发送订阅请求。"""
    if exchange == "bybit":
        payload = {"op": "subscribe", "args": [f"orderbook.{depth}.{symbol}"]}
        ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return
    if exchange == "bitget":
        inst_type = "USDT-FUTURES" if market == "future" else "SPOT"
        payload = {"op": "subscribe", "args": [{"instType": inst_type, "channel": "books", "instId": symbol}]}
        ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return
    if exchange == "okx":
        payload = {"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}
        ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))


def ping_payload(exchange: str) -> str | None:
    """返回指定交易所的心跳内容。"""
    if exchange == "bybit":
        return json.dumps({"op": "ping"}, ensure_ascii=True, separators=(",", ":"))
    if exchange in {"bitget", "okx"}:
        return "ping"
    return None


def normalize_bybit_raw(message: dict, collect_ts: int, market: str, symbol: str) -> dict:
    """为Bybit原始消息补齐统一字段。"""
    payload = dict(message)
    payload["symbol"] = payload.get("data", {}).get("s", symbol)
    payload["collect_ts"] = collect_ts
    return payload


def normalize_binance_raw(message: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将Binance原始消息归一化为Bybit样式。"""
    ts_ms = int(message.get("E", "0") or 0)
    cts_ms = int(message.get("T", ts_ms) or ts_ms)
    update_id = int(message.get("u", "0") or 0)
    return {
        "topic": f"orderbook.{depth}.{symbol}",
        "symbol": symbol,
        "type": "snapshot",
        "ts": ts_ms,
        "cts": cts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": symbol,
            "b": message.get("b", []),
            "a": message.get("a", []),
            "u": update_id,
            "seq": update_id,
        },
    }


def normalize_bitget_raw(message: dict, item: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将Bitget原始消息归一化为Bybit样式。"""
    inst_id = message.get("arg", {}).get("instId", symbol)
    seq = int(item.get("seq", "0") or 0)
    ts_ms = int(item.get("ts", "0") or 0)
    action = message.get("action", "")
    return {
        "topic": f"orderbook.{depth}.{inst_id}",
        "symbol": inst_id,
        "type": "snapshot" if action == "snapshot" else "delta",
        "ts": ts_ms,
        "cts": ts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": inst_id,
            "b": item.get("bids", []),
            "a": item.get("asks", []),
            "u": seq,
            "seq": seq,
        },
    }


def normalize_okx_raw(message: dict, item: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将OKX原始消息归一化为Bybit样式。"""
    inst_id = message.get("arg", {}).get("instId", symbol)
    asks = normalize_okx_levels(item.get("asks", []))
    bids = normalize_okx_levels(item.get("bids", []))
    update_id = int(item.get("seqId", item.get("ts", "0")) or 0)
    previous_id = int(item.get("prevSeqId", update_id) or update_id)
    ts_ms = int(item.get("ts", "0") or 0)
    action = message.get("action", "")
    return {
        "topic": f"orderbook.{depth}.{inst_id}",
        "symbol": inst_id,
        "type": "snapshot" if action == "snapshot" else "delta",
        "ts": ts_ms,
        "cts": ts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": inst_id,
            "b": bids,
            "a": asks,
            "u": update_id,
            "seq": previous_id,
        },
    }


def apply_bybit_message(orderbook: dict, market: str, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[dict, bool]:
    """处理Bybit消息并生成快照。"""
    raw_record = normalize_bybit_raw(message, collect_ts, market, symbol)
    msg_type = message.get("type", "")
    data = message.get("data", {})
    if msg_type == "snapshot":
        replace_orderbook(orderbook, data.get("b", []), data.get("a", []))
    elif msg_type == "delta":
        apply_orderbook_delta(orderbook, data.get("b", []), data.get("a", []))
    else:
        return raw_record, False
    trim_orderbook(orderbook)
    snapshot = build_snapshot(
        "bybit",
        market,
        data.get("s", symbol),
        orderbook,
        msg_type,
        int(message.get("ts", "0") or 0),
        int(message.get("cts", message.get("ts", "0")) or 0),
        collect_ts,
        int(data.get("u", "0") or 0),
        int(data.get("seq", data.get("u", "0")) or 0),
        depth,
    )
    return raw_record, snapshot


def apply_binance_message(
    orderbook: dict,
    market: str,
    symbol: str,
    message: dict,
    collect_ts: int,
    depth: int,
) -> tuple[dict, dict]:
    """处理Binance消息并生成快照。"""
    raw_record = normalize_binance_raw(message, collect_ts, message.get("s", symbol), depth)
    replace_orderbook(orderbook, message.get("b", []), message.get("a", []))
    trim_orderbook(orderbook)
    snapshot = build_snapshot(
        "binance",
        market,
        message.get("s", symbol),
        orderbook,
        "snapshot",
        int(message.get("E", "0") or 0),
        int(message.get("T", message.get("E", "0")) or 0),
        collect_ts,
        int(message.get("u", "0") or 0),
        int(message.get("u", "0") or 0),
        depth,
    )
    return raw_record, snapshot


def apply_bitget_message(orderbook: dict, market: str, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[list, list]:
    """处理Bitget消息并生成快照列表。"""
    raw_records = []
    if message.get("event") == "subscribe":
        return raw_records, []
    action = message.get("action", "")
    snapshots = []
    for item in message.get("data", []):
        raw_records.append(normalize_bitget_raw(message, item, collect_ts, symbol, depth))
        if action == "snapshot":
            replace_orderbook(orderbook, item.get("bids", []), item.get("asks", []))
        elif action == "update":
            apply_orderbook_delta(orderbook, item.get("bids", []), item.get("asks", []))
        else:
            continue
        trim_orderbook(orderbook)
        ts_ms = int(item.get("ts", "0") or 0)
        seq = int(item.get("seq", "0") or 0)
        snapshots.append(
            build_snapshot(
                "bitget",
                market,
                message.get("arg", {}).get("instId", symbol),
                orderbook,
                "snapshot" if action == "snapshot" else "delta",
                ts_ms,
                ts_ms,
                collect_ts,
                seq if seq else None,
                seq if seq else None,
                depth,
            )
        )
    return raw_records, snapshots


def normalize_okx_levels(levels: list) -> list:
    """裁剪OKX盘口字段为价格和数量。"""
    return [[level[0], level[1]] for level in levels if len(level) >= 2]


def apply_okx_message(orderbook: dict, market: str, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[list, list]:
    """处理OKX消息并生成快照列表。"""
    raw_records = []
    if message.get("event") == "subscribe":
        return raw_records, []
    action = message.get("action", "")
    snapshots = []
    for item in message.get("data", []):
        raw_records.append(normalize_okx_raw(message, item, collect_ts, symbol, depth))
        asks = normalize_okx_levels(item.get("asks", []))
        bids = normalize_okx_levels(item.get("bids", []))
        if action == "snapshot":
            replace_orderbook(orderbook, bids, asks)
        elif action == "update":
            apply_orderbook_delta(orderbook, bids, asks)
        else:
            continue
        trim_orderbook(orderbook)
        ts_ms = int(item.get("ts", "0") or 0)
        update_id = int(item.get("seqId", item.get("ts", "0")) or 0)
        previous_id = int(item.get("prevSeqId", update_id) or update_id)
        snapshots.append(
            build_snapshot(
                "okx",
                market,
                message.get("arg", {}).get("instId", symbol),
                orderbook,
                "snapshot" if action == "snapshot" else "delta",
                ts_ms,
                ts_ms,
                collect_ts,
                update_id if update_id else None,
                previous_id if previous_id else None,
                depth,
            )
        )
    return raw_records, snapshots


def write_json_line(writer, payload: dict) -> None:
    """写入单行JSON记录。"""
    writer[2].write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")


def flush_second_snapshot(last_snapshot: dict | None, writer, base_dir: Path, tag: str, symbol: str):
    """将上一秒快照写入秒级文件。"""
    if not last_snapshot:
        return writer
    snapshot_hour = hour_str_from_ms(last_snapshot["collect_ts"])
    writer = ensure_writer(base_dir, symbol, snapshot_hour, tag, writer)
    write_json_line(writer, last_snapshot)
    return writer


def run_session(exchange: str, market: str, symbol: str, role: str, ws_url: str, stop_event: threading.Event, state: dict) -> None:
    """运行单个角色的一次WS会话。"""
    depth = app_config.ORDERBOOK_DEPTH_FUTURE if market == "future" else app_config.ORDERBOOK_DEPTH_SPOT
    rt_dir, rt_ss_dir, rt_ss_1s_dir, rt_tag, rt_ss_tag, rt_ss_1s_tag = build_dirs(exchange, market)
    try:
        ws = connect_ws(ws_url)
        send_subscribe(ws, exchange, market, symbol, depth)
        update_shared_status(state, exchange, market, symbol, role, connected=True, status_text="已连接 0", recv_count=0)
    except websocket.WebSocketException as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接异常")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}连接异常，准备重连: {exc}", market)
        return
    except TimeoutError as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接超时")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}连接超时，准备重连: {exc}", market)
        return
    except OSError as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="网络错误")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}网络错误，准备重连: {exc}", market)
        return

    orderbook = {"bids": {}, "asks": {}}
    rt_writer = None
    rt_ss_writer = None
    rt_ss_1s_writer = None
    recv_count = 0
    last_status_ts = time.monotonic()
    last_second = None
    last_snapshot = None
    heartbeat_closed = threading.Event()

    def keepalive() -> None:
        """后台发送心跳包。"""
        payload = ping_payload(exchange)
        if payload is None:
            return
        while not stop_event.is_set() and not heartbeat_closed.is_set():
            time.sleep(PING_INTERVAL_SECONDS)
            if stop_event.is_set() or heartbeat_closed.is_set():
                return
            try:
                ws.send(payload)
            except websocket.WebSocketException:
                return
            except TimeoutError:
                return
            except OSError:
                return

    keepalive_thread = threading.Thread(target=keepalive, daemon=True)
    keepalive_thread.start()

    while not stop_event.is_set():
        try:
            raw = ws.recv()
        except websocket.WebSocketException as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接异常")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}连接异常，准备重连: {exc}", market)
            break
        except TimeoutError as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接超时")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}连接超时，准备重连: {exc}", market)
            break
        except OSError as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="网络错误")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}网络错误，准备重连: {exc}", market)
            break
        if raw == "pong":
            continue
        recv_count += 1
        now_status_ts = time.monotonic()
        if now_status_ts - last_status_ts >= STATUS_INTERVAL_SECONDS:
            update_shared_status(state, exchange, market, symbol, role, connected=True, status_text=f"已连接 {recv_count}", recv_count=recv_count)
            last_status_ts = now_status_ts
        collect_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        message = json.loads(raw)
        snapshots = []
        if exchange == "bybit":
            raw_record, snapshot = apply_bybit_message(orderbook, market, symbol, message, collect_ts, depth)
            if snapshot:
                snapshots.append(snapshot)
            raw_records = [raw_record]
        elif exchange == "binance":
            raw_record, snapshot = apply_binance_message(orderbook, market, symbol, message, collect_ts, depth)
            snapshots.append(snapshot)
            raw_records = [raw_record]
        elif exchange == "bitget":
            raw_records, snapshots = apply_bitget_message(orderbook, market, symbol, message, collect_ts, depth)
        else:
            raw_records, snapshots = apply_okx_message(orderbook, market, symbol, message, collect_ts, depth)

        active_now = is_active_role(state, role)
        if not active_now:
            close_writer(rt_writer)
            close_writer(rt_ss_writer)
            close_writer(rt_ss_1s_writer)
            rt_writer = None
            rt_ss_writer = None
            rt_ss_1s_writer = None
        else:
            hour_str = hour_str_from_ms(collect_ts)
            rt_writer = ensure_writer(rt_dir, symbol, hour_str, rt_tag, rt_writer)
            for raw_record in raw_records:
                write_json_line(rt_writer, raw_record)
            for snapshot in snapshots:
                snapshot_hour = hour_str_from_ms(snapshot["collect_ts"])
                rt_ss_writer = ensure_writer(rt_ss_dir, symbol, snapshot_hour, rt_ss_tag, rt_ss_writer)
                write_json_line(rt_ss_writer, snapshot)
                second_bucket = int(snapshot["collect_ts"] / 1000)
                if last_second is None:
                    last_second = second_bucket
                if second_bucket != last_second and last_snapshot:
                    rt_ss_1s_writer = flush_second_snapshot(last_snapshot, rt_ss_1s_writer, rt_ss_1s_dir, rt_ss_1s_tag, symbol)
                    last_second = second_bucket
                last_snapshot = snapshot

    heartbeat_closed.set()
    if last_snapshot and is_active_role(state, role):
        rt_ss_1s_writer = flush_second_snapshot(last_snapshot, rt_ss_1s_writer, rt_ss_1s_dir, rt_ss_1s_tag, symbol)
    close_writer(rt_writer)
    close_writer(rt_ss_writer)
    close_writer(rt_ss_1s_writer)
    ws.close()
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接关闭")
    switch_active_role(state, exchange, market, symbol, role)


def run_role_loop(exchange: str, market: str, symbol: str, role: str, ws_url: str, stop_event: threading.Event, state: dict) -> None:
    """持续维护单个角色的WS连接。"""
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="准备连接", recv_count=0)
    while not stop_event.is_set():
        run_session(exchange, market, symbol, role, ws_url, stop_event, state)
        if stop_event.is_set():
            break
        time.sleep(RECONNECT_INTERVAL_SECONDS)
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="已停止")


def run_symbol_loop(exchange: str, market: str, symbol: str, stop_event: threading.Event) -> None:
    """持续维护单个交易对的主备WS连接。"""
    log(f"{exchange} {market} {symbol} 主备订阅启动", market)
    state = build_session_state()
    primary_url, backup_url = build_ws_urls(exchange, market, symbol)
    primary_thread = threading.Thread(
        target=run_role_loop,
        args=(exchange, market, symbol, PRIMARY_ROLE, primary_url, stop_event, state),
        daemon=True,
    )
    backup_thread = threading.Thread(
        target=run_role_loop,
        args=(exchange, market, symbol, BACKUP_ROLE, backup_url, stop_event, state),
        daemon=True,
    )
    primary_thread.start()
    backup_thread.start()
    primary_thread.join()
    backup_thread.join()
    status_update(exchange, market, symbol, None)
    log(f"{exchange} {market} {symbol} 主备订阅停止", market)


def run_exchange_supervisor(exchange: str, market: str) -> None:
    """按交易所维护全部交易对子线程。"""
    if not cex_config.is_supported("D10002-4" if market == "future" else "D10006-8", exchange):
        symbol_list = cex_config.get_future_symbols(exchange) if market == "future" else cex_config.get_spot_symbols(exchange)
        for symbol in symbol_list:
            status_update(exchange, market, symbol, cex_config.UNSUPPORTED_STATUS_TEXT)
        return
    workers: dict[str, tuple[threading.Event, threading.Thread]] = {}
    fallback_symbols = cex_config.get_future_symbols(exchange) if market == "future" else cex_config.get_spot_symbols(exchange)
    while True:
        try:
            desired_symbols = resolve_symbols(exchange, market)
        except NetworkRequestError as exc:
            desired_symbols = fallback_symbols
            log(f"{exchange} {market} 动态合约刷新失败，继续使用静态列表: {exc}", market)
        desired_set = set(desired_symbols)
        for symbol in sorted(desired_set - set(workers)):
            stop_event = threading.Event()
            thread = threading.Thread(target=run_symbol_loop, args=(exchange, market, symbol, stop_event), daemon=True)
            workers[symbol] = (stop_event, thread)
            thread.start()
        for symbol in sorted(set(workers) - desired_set):
            stop_event, _thread = workers.pop(symbol)
            stop_event.set()
        time.sleep(DELIVERY_REFRESH_SECONDS if market == "future" else max(DELIVERY_REFRESH_SECONDS, 60))


def run_market_ws(market: str) -> None:
    """运行指定市场的多交易所订单簿WS。"""
    supervisors = []
    for exchange in cex_config.list_exchanges():
        thread = threading.Thread(target=run_exchange_supervisor, args=(exchange, market), daemon=True)
        supervisors.append(thread)
        thread.start()
    for thread in supervisors:
        thread.join()
