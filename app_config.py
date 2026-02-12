BYBIT_SYMBOL = (
    "BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT, DOGEUSDT, TRXUSDT, SUIUSDT, PEPEUSDT, HBARUSDT"
    ", AAVEUSDT, HYPEUSDT, ADAUSDT, BCHUSDT, ZECUSDT, LINKUSDT, LTCUSDT, UNIUSDT, SHIBUSDT, TONUSDT"
)  # Bybit交易对，字符串
BYBIT_INSURANCE_COINS = ["USDT"]  # 保险基金币种列表，个数
BYBIT_ONCHAIN_COINS = []  # 链上理财币种过滤列表，个数
BYBIT_FUTURE_DELIVERY_CATEGORIES = ["linear"]  # 交割期货产品类型列表，个数
BYBIT_FUTURE_DELIVERY_STATUSES = ["Trading", "PreLaunch", "Delivering", "Closed"]  # 交割期货状态列表，个数
BYBIT_FUTURE_DELIVERY_EXCLUDE = ["MNTUSDT"]  # 交割合约过滤列表，个数
POLYMARKET_EVENT_TEMPLATES = [
    "https://polymarket.com/event/{name}-up-or-down-on-{month}-{day}",
    "https://polymarket.com/event/{symbol}-updown-4h-{epoch_4h}",
    "https://polymarket.com/event/{name}-up-or-down-{month}-{day}-{hour12}{ampm}-et",
    "https://polymarket.com/event/{symbol}-updown-15m-{epoch_15m}",
    "https://polymarket.com/event/{symbol}-updown-5m-{epoch_5m}",
    "https://polymarket.com/event/what-price-will-bitcoin-hit-in-february-2026",
]  # Polymarket事件模板列表，个数
POLYMARKET_ASSET_TAGS = [
    "bitcoin:btc:1",
    "ethereum:eth:0",
    "solana:sol:0",
    "xrp:xrp:0",
]  # Polymarket资产映射列表，个数
POLYMARKET_TZ_NAME = "America/New_York"  # Polymarket时区名称，时区

D10001_START_DATE = "2026-02-08"  # D10001起始日期，日期
D10005_START_DATE = "2026-02-08"  # D10005起始日期，日期
D10013_START_DATE = "2026-02-08"  # D10013起始日期，日期
D10014_START_DATE = "2026-02-08"  # D10014起始日期，日期
D10017_START_DATE = "2026-02-08"  # D10017起始日期，日期

SCHEDULE_REFRESH_SECONDS = 10  # 触发时间刷新间隔，秒
WS_STATUS_STALE_SECONDS = 15  # WS状态超时，秒

INSTRUMENTS_TIMEOUT_SECONDS = 10  # 交易对接口超时，秒
INSTRUMENTS_LIMIT = 1000  # 交易对接口分页大小，条
HTTP_TIMEOUT_SECONDS = 10  # HTTP请求超时，秒
DOWNLOAD_TIMEOUT_SECONDS = 30  # 下载超时，秒
RETRY_TIMES = 5  # 最大重试次数，次
RETRY_INTERVAL_SECONDS = 5  # 重试间隔，秒
CHUNK_SIZE = 1024 * 1024  # 下载块大小，字节
LOOP_INTERVAL_SECONDS = 4 * 60 * 60  # 循环间隔，秒
DELIVERY_REFRESH_SECONDS = 15 * 60  # 交割合约刷新间隔，秒

WS_TIMEOUT_SECONDS = 10  # WS连接超时，秒
WS_RECV_TIMEOUT_SECONDS = 30  # WS接收超时，秒
WS_RECONNECT_INTERVAL_SECONDS = 2  # WS重连间隔，秒
WS_STATUS_INTERVAL_SECONDS = 1  # WS状态输出间隔，秒
WS_PING_INTERVAL_SECONDS = 10  # WS心跳间隔，秒

ORDERBOOK_DEPTH_FUTURE = 200  # 期货订单簿深度，档位
ORDERBOOK_DEPTH_SPOT = 50  # 现货订单簿深度，档位

PRECONNECT_LEAD_SECONDS = 60  # 预连接提前量，秒
BECOME_ACTIVE_AFTER_SECONDS = 90  # 预连接最大等待，秒
SCHEDULER_TICK_SECONDS = 1  # 调度循环间隔，秒
BATCH_SIZE = 2000  # 单文件最大记录数，条

START_TASKS = []  # 启动任务列表，个数
TUI_REFRESH_SECONDS = 0.1  # TUI刷新间隔，秒
LOG_LINES_PER_TASK = 50  # 每任务日志行数，行
ERROR_LOG_PATH = "logs/error.log"  # 错误日志路径，路径
ERROR_LOG_KEYWORDS = ["错误", "异常", "Traceback", "Exception"]  # 错误关键词列表，个数
ERROR_LOG_EXCLUDE_KEYWORDS = ["HTTP错误404", "HTTP Error 404"]  # 错误排除关键词列表，个数


def parse_bybit_symbols(text: str) -> list:
    return [item.strip() for item in text.split(",") if item.strip()]
