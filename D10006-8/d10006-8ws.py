from cex import cex_orderbook_ws_common as orderbook_ws_common


QUIET = globals().get("QUIET", False)  # 静默模式开关，开关
STATUS_HOOK = globals().get("STATUS_HOOK")  # 状态回调函数，函数
LOG_HOOK = globals().get("LOG_HOOK")  # 日志回调函数，函数


def run() -> None:
    """运行现货订单簿WS任务。"""
    orderbook_ws_common.QUIET = QUIET
    orderbook_ws_common.STATUS_HOOK = STATUS_HOOK
    orderbook_ws_common.LOG_HOOK = LOG_HOOK
    orderbook_ws_common.run_market_ws("spot")


if __name__ == "__main__":
    run()
