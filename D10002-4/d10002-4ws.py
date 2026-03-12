from cex import cex_orderbook_ws_common as orderbook_ws_common


QUIET = False  # 静默模式开关，开关
STATUS_HOOK = None  # 状态回调函数，函数
LOG_HOOK = None  # 日志回调函数，函数


def run() -> None:
    """运行期货订单簿WS任务。"""
    orderbook_ws_common.QUIET = QUIET
    orderbook_ws_common.STATUS_HOOK = STATUS_HOOK
    orderbook_ws_common.LOG_HOOK = LOG_HOOK
    orderbook_ws_common.run_market_ws("future")


if __name__ == "__main__":
    run()
