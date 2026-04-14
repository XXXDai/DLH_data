from pathlib import Path
import sys

import launcher
from cex import cex_config


DEFAULT_RAW_FUTURE_SCOPE = "-rawfuture=okx,binance:ETH:2026-02-01:2026-03-31"  # 默认原始期货专项范围，字符串


def prepare_runtime_args() -> None:
    """补齐独立启动器需要的默认参数。"""
    if "-s3" not in sys.argv:
        sys.argv.append("-s3")
    if "-nowss" not in sys.argv:
        sys.argv.append("-nowss")
    if not any(arg.startswith("-rawfuture=") for arg in sys.argv):
        sys.argv.append(DEFAULT_RAW_FUTURE_SCOPE)


def list_no_wait_launcher_processes() -> list[str]:
    """独立启动器不等待旧进程切换。"""
    return []


def build_raw_future_symbol(exchange: str, base_coin: str) -> str:
    """构造原始期货专项交易对目录名。"""
    if exchange == "okx":
        return f"{base_coin}-USDT-SWAP"
    return f"{base_coin}USDT"


def build_raw_future_orderbook_path(exchange: str, base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造原始期货订单簿文件路径。"""
    if exchange == "bybit":
        return base_dir / symbol / f"{date_str}_{symbol}_ob200.data.zip"
    if exchange == "binance":
        return base_dir / symbol / f"{symbol}-bookTicker-{date_str}.zip"
    if exchange == "bitget":
        return base_dir / symbol / f"{date_str.replace('-', '')}.zip"
    return base_dir / symbol / f"{date_str}_{symbol}_ob400.data.zip"


def build_raw_future_trade_path(base_dir: Path, symbol: str, date_str: str) -> Path:
    """构造原始期货成交文件路径。"""
    return base_dir / symbol / f"{symbol}{date_str}.csv.gz"


def build_raw_future_upload_scan_roots() -> list[Path]:
    """构造原始期货专项上传扫描文件列表。"""
    scope = launcher.parse_future_raw_priority_scope()
    if not scope:
        return []
    paths = []
    for exchange in scope["exchanges"]:
        symbol = build_raw_future_symbol(exchange, scope["base_coin"])
        orderbook_base_dir = cex_config.get_source_dir("D10001", exchange)
        trade_base_dir = cex_config.get_source_dir("D10013", exchange)
        for date_str in launcher.cex_common.iter_dates(scope["start_date"], scope["end_date"]):
            if orderbook_base_dir:
                paths.append(build_raw_future_orderbook_path(exchange, orderbook_base_dir, symbol, date_str))
            if trade_base_dir:
                paths.append(build_raw_future_trade_path(trade_base_dir, symbol, date_str))
    return paths


def main() -> None:
    """运行原始期货专项独立启动器。"""
    prepare_runtime_args()
    launcher.apply_storage_mode_from_argv()
    launcher.cex_common.set_upload_startup_sync_enabled(True)
    launcher.cex_common.set_upload_startup_scan_roots(build_raw_future_upload_scan_roots())
    launcher.cex_common.set_storage_s3_read_enabled(True)
    launcher.list_other_launcher_processes = list_no_wait_launcher_processes
    launcher.main()


if __name__ == "__main__":
    main()
