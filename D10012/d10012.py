from cex import cex_orderbook_snapshot_common as orderbook_snapshot_common


QUIET = False  # 静默模式开关，开关
LOG_HOOK = None  # 日志回调函数，函数


def run() -> None:
    """运行现货历史订单簿快照任务。"""
    orderbook_snapshot_common.QUIET = QUIET
    orderbook_snapshot_common.LOG_HOOK = LOG_HOOK
    orderbook_snapshot_common.run_dataset("D10005", "D10012")


if __name__ == "__main__":
    run()
