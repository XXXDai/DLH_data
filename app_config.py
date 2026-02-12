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
]  # Polymarket事件模板列表，个数
POLYMARKET_ASSET_TAGS = [
    "bitcoin:btc:1",
    "ethereum:eth:0",
    "solana:sol:0",
    "xrp:xrp:0",
]  # Polymarket资产映射列表，个数
POLYMARKET_TZ_NAME = "America/New_York"  # Polymarket时区名称，时区

D10001_START_DATE = "2026-02-10"  # D10001起始日期，日期
D10005_START_DATE = "2026-02-10"  # D10005起始日期，日期
D10013_START_DATE = "2026-02-10"  # D10013起始日期，日期
D10014_START_DATE = "2026-02-10"  # D10014起始日期，日期
D10017_START_DATE = "2026-02-10"  # D10017起始日期，日期

START_TASKS = []  # 启动任务列表，个数
TUI_REFRESH_SECONDS = 0.1  # TUI刷新间隔，秒
LOG_LINES_PER_TASK = 50  # 每任务日志行数，行
ERROR_LOG_PATH = "logs/error.log"  # 错误日志路径，路径
ERROR_LOG_KEYWORDS = ["错误", "异常", "Traceback", "Exception"]  # 错误关键词列表，个数
ERROR_LOG_EXCLUDE_KEYWORDS = ["HTTP错误404", "HTTP Error 404"]  # 错误排除关键词列表，个数


def parse_bybit_symbols(text: str) -> list:
    return [item.strip() for item in text.split(",") if item.strip()]
