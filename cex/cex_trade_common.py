from datetime import datetime, timezone


NORMALIZED_TRADE_FIELDS = [
    "timestamp",  # 成交时间戳字段，字符串
    "symbol",  # 交易对字段，字符串
    "side",  # 方向字段，字符串
    "size",  # 成交数量字段，字符串
    "price",  # 成交价格字段，字符串
    "quote_qty",  # 成交额字段，字符串
    "trade_id",  # 成交编号字段，字符串
    "exchange",  # 交易所字段，字符串
]  # 标准成交字段列表，个数


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
    ts_ms = int(row["timestamp"])
    if ts_ms > 10**14:
        ts_ms //= 1000
    return ts_ms, float(row["price"]), float(row["size"])


def parse_okx_trade_row(row: dict) -> tuple[int, float, float]:
    """解析OKX成交毫秒字段。"""
    return int(row["timestamp"]), float(row["price"]), float(row["size"])


def parse_bitget_trade_row(row: dict) -> tuple[int, float, float]:
    """解析Bitget成交毫秒字段。"""
    size_value = row.get("size")
    if size_value in {None, ""}:
        size_value = row.get("size(base)")
    return int(row["timestamp"]), float(row["price"]), float(size_value)


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
        "timestamp": str(ts_ms),
        "symbol": symbol,
        "side": "Sell" if parts[5].lower() == "true" else "Buy",
        "size": size,
        "price": price,
        "quote_qty": parts[3],
        "trade_id": parts[0],
        "exchange": "binance",
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
        "timestamp": str(ts_ms),
        "symbol": symbol,
        "side": "Sell" if parts[5].lower() == "true" else "Buy",
        "size": size,
        "price": price,
        "quote_qty": parts[3],
        "trade_id": parts[0],
        "exchange": "binance",
    }
    return ts_ms_to_date(ts_ms), row


def normalize_okx_trade_row(row: dict) -> tuple[str, dict] | None:
    """标准化OKX成交行。"""
    created_time = row.get("created_time")
    if not created_time:
        return None
    ts_ms = int(created_time)
    normalized = {
        "timestamp": created_time,
        "symbol": row.get("instrument_name", ""),
        "side": row.get("side", ""),
        "size": row.get("size", ""),
        "price": row.get("price", ""),
        "quote_qty": "",
        "trade_id": row.get("trade_id", ""),
        "exchange": "okx",
    }
    return ts_ms_to_date(ts_ms), normalized


def normalize_bitget_trade_row(symbol: str, row: dict) -> tuple[str, dict] | None:
    """标准化Bitget现货成交行。"""
    timestamp = row.get("timestamp")
    if not timestamp:
        return None
    ts_ms = int(timestamp)
    normalized = {
        "timestamp": timestamp,
        "symbol": symbol,
        "side": "Buy" if str(row.get("side", "")).lower() == "buy" else "Sell",
        "size": row.get("size(base)", ""),
        "price": row.get("price", ""),
        "quote_qty": row.get("volume(quote)", ""),
        "trade_id": row.get("trade_id", ""),
        "exchange": "bitget",
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
