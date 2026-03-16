from datetime import datetime, timezone


FUTURE_TRADE_FIELDS = [
    "timestamp",  # 成交时间戳字段，字符串
    "symbol",  # 交易对字段，字符串
    "side",  # 方向字段，字符串
    "size",  # 成交数量字段，字符串
    "price",  # 成交价格字段，字符串
    "tickDirection",  # 价格变动方向字段，字符串
    "trdMatchID",  # 成交编号字段，字符串
    "grossValue",  # 原始成交总价值字段，字符串
    "homeNotional",  # 基础币数量字段，字符串
    "foreignNotional",  # 计价币成交额字段，字符串
    "RPI",  # 做市保护标记字段，字符串
]  # 期货成交字段列表，个数
SPOT_TRADE_FIELDS = [
    "id",  # 成交编号字段，字符串
    "timestamp",  # 成交时间戳字段，字符串
    "price",  # 成交价格字段，字符串
    "volume",  # 成交数量字段，字符串
    "side",  # 方向字段，字符串
]  # 现货成交字段列表，个数


def format_timestamp_seconds(ts_ms: int) -> str:
    """将毫秒时间戳格式化为秒级小数字符串。"""
    return f"{ts_ms / 1000:.3f}"


def format_quote_value(price_text: str, size_text: str) -> str:
    """计算计价币成交额字符串。"""
    return format(float(price_text) * float(size_text), "f")


def format_timestamp_ms(ts_ms: int) -> str:
    """将毫秒时间戳格式化为整数字符串。"""
    return str(ts_ms)


def ts_ms_to_date(ts_ms: int) -> str:
    """将毫秒时间戳转换为UTC日期。"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def parse_bybit_trade_row(row: dict) -> tuple[int, float, float]:
    """解析Bybit成交毫秒字段。"""
    ts_value = float(row["timestamp"])
    ts_ms = int(ts_value) if ts_value > 1e12 else int(ts_value * 1000)
    size_value = row.get("size")
    if size_value in {None, ""}:
        size_value = row.get("volume")
    return ts_ms, float(row["price"]), float(size_value)


def parse_binance_trade_row(row: dict) -> tuple[int, float, float]:
    """解析Binance成交毫秒字段。"""
    return parse_bybit_trade_row(row)


def parse_okx_trade_row(row: dict) -> tuple[int, float, float]:
    """解析OKX成交毫秒字段。"""
    return parse_bybit_trade_row(row)


def parse_bitget_trade_row(row: dict) -> tuple[int, float, float]:
    """解析Bitget成交毫秒字段。"""
    return parse_bybit_trade_row(row)


def normalize_binance_spot_parts(symbol: str, parts: list[str]) -> tuple[str, dict] | None:
    """标准化Binance现货成交行。"""
    if len(parts) < 7:
        return None
    if not str(parts[4]).isdigit():
        return None
    ts_ms = int(parts[4])
    if ts_ms > 10**14:
        ts_ms //= 1000
    size = parts[2]
    price = parts[1]
    row = {
        "id": parts[0],
        "timestamp": format_timestamp_ms(ts_ms),
        "side": "Sell" if parts[5].lower() == "true" else "Buy",
        "price": price,
        "volume": size,
    }
    return ts_ms_to_date(ts_ms), row


def normalize_binance_future_parts(symbol: str, parts: list[str]) -> tuple[str, dict] | None:
    """标准化Binance期货成交行。"""
    if len(parts) < 6:
        return None
    if not str(parts[4]).isdigit():
        return None
    ts_ms = int(parts[4])
    if ts_ms > 10**14:
        ts_ms //= 1000
    size = parts[2]
    price = parts[1]
    row = {
        "timestamp": format_timestamp_seconds(ts_ms),
        "symbol": symbol,
        "side": "Sell" if parts[5].lower() == "true" else "Buy",
        "size": size,
        "price": price,
        "tickDirection": "",
        "trdMatchID": parts[0],
        "grossValue": format_quote_value(price, size),
        "homeNotional": size,
        "foreignNotional": parts[3],
        "RPI": "",
    }
    return ts_ms_to_date(ts_ms), row


def normalize_okx_future_trade_row(row: dict) -> tuple[str, dict] | None:
    """标准化OKX期货成交行。"""
    created_time = row.get("created_time")
    if not created_time:
        return None
    ts_ms = int(created_time)
    size = str(row.get("size", ""))
    price = str(row.get("price", ""))
    normalized = {
        "timestamp": format_timestamp_seconds(ts_ms),
        "symbol": row.get("instrument_name", ""),
        "side": str(row.get("side", "")).capitalize(),
        "size": size,
        "price": price,
        "tickDirection": "",
        "trdMatchID": row.get("trade_id", ""),
        "grossValue": format_quote_value(price, size) if price and size else "",
        "homeNotional": size,
        "foreignNotional": format_quote_value(price, size) if price and size else "",
        "RPI": "",
    }
    return ts_ms_to_date(ts_ms), normalized


def normalize_okx_spot_trade_row(row: dict) -> tuple[str, dict] | None:
    """标准化OKX现货成交行。"""
    created_time = row.get("created_time")
    if not created_time:
        return None
    ts_ms = int(created_time)
    size = str(row.get("size", ""))
    normalized = {
        "id": row.get("trade_id", ""),
        "timestamp": format_timestamp_ms(ts_ms),
        "price": str(row.get("price", "")),
        "volume": size,
        "side": str(row.get("side", "")).capitalize(),
    }
    return ts_ms_to_date(ts_ms), normalized


def normalize_bitget_future_trade_row(symbol: str, row: dict) -> tuple[str, dict] | None:
    """标准化Bitget期货成交行。"""
    timestamp = row.get("timestamp")
    if not timestamp:
        return None
    ts_ms = int(timestamp)
    size = str(row.get("size(base)", ""))
    price = str(row.get("price", ""))
    quote_qty = str(row.get("volume(quote)", ""))
    normalized = {
        "timestamp": format_timestamp_seconds(ts_ms),
        "symbol": symbol,
        "side": "Buy" if str(row.get("side", "")).lower() == "buy" else "Sell",
        "size": size,
        "price": price,
        "tickDirection": "",
        "trdMatchID": row.get("trade_id", ""),
        "grossValue": quote_qty or (format_quote_value(price, size) if price and size else ""),
        "homeNotional": size,
        "foreignNotional": quote_qty or (format_quote_value(price, size) if price and size else ""),
        "RPI": "",
    }
    return ts_ms_to_date(ts_ms), normalized


def normalize_bitget_spot_trade_row(symbol: str, row: dict) -> tuple[str, dict] | None:
    """标准化Bitget现货成交行。"""
    timestamp = row.get("timestamp")
    if not timestamp:
        return None
    ts_ms = int(timestamp)
    normalized = {
        "id": row.get("trade_id", ""),
        "timestamp": format_timestamp_ms(ts_ms),
        "price": str(row.get("price", "")),
        "volume": str(row.get("size(base)", "")),
        "side": "Buy" if str(row.get("side", "")).lower() == "buy" else "Sell",
    }
    return ts_ms_to_date(ts_ms), normalized


def normalize_trade_for_agg(exchange: str, row: dict) -> tuple[int, float, float]:
    """按交易所解析聚合所需毫秒字段。"""
    if exchange == "bybit":
        return parse_bybit_trade_row(row)
    if exchange == "binance":
        return parse_binance_trade_row(row)
    if exchange == "bitget":
        return parse_bitget_trade_row(row)
    if exchange == "okx":
        return parse_okx_trade_row(row)
    raise ValueError(f"不支持的交易所: {exchange}")
