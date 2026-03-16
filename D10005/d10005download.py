from cex import cex_orderbook_archive_common as orderbook_archive_common
from cex import cex_orderbook_snapshot_common as orderbook_snapshot_common


QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数


def run() -> None:
    """运行现货历史订单簿下载任务。"""
    orderbook_archive_common.configure_market_runtime("spot", QUIET, STATUS_HOOK, LOG_HOOK)
    orderbook_snapshot_common.configure_dataset_runtime("D10012", QUIET, STATUS_HOOK, LOG_HOOK)
    orderbook_archive_common.run_market("spot")


if __name__ == "__main__":
    run()
