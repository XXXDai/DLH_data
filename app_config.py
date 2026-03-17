SCHEDULE_REFRESH_SECONDS = 10  # 触发时间刷新间隔，秒
WS_STATUS_STALE_SECONDS = 15  # WS状态超时，秒

INSTRUMENTS_TIMEOUT_SECONDS = 10  # 交易对接口超时，秒
INSTRUMENTS_LIMIT = 1000  # 交易对接口分页大小，条
HTTP_TIMEOUT_SECONDS = 10  # HTTP请求超时，秒
DOWNLOAD_TIMEOUT_SECONDS = 30  # 下载超时，秒
RETRY_TIMES = 5  # 最大重试次数，次
RETRY_INTERVAL_SECONDS = 5  # 重试间隔，秒
CHUNK_SIZE = 1024 * 1024  # 下载块大小，字节
DOWNLOAD_CONCURRENCY = 4  # 下载并发数，个数
LOOP_INTERVAL_SECONDS = 4 * 60 * 60  # 循环间隔，秒
DELIVERY_REFRESH_SECONDS = 15 * 60  # 交割合约刷新间隔，秒
DATA_STORAGE_MODE = "local"  # 数据存储模式，可选local或s3，字符串
S3_BUCKET_NAME = "main-ai-ext"  # S3桶名称，字符串
S3_PREFIX = "dlh/data"  # S3目录前缀，路径
S3_CONNECT_TIMEOUT_SECONDS = 10  # S3连接超时，秒
S3_READ_TIMEOUT_SECONDS = 60  # S3读取超时，秒
S3_MAX_ATTEMPTS = 3  # S3最大重试次数，次
S3_MAX_POOL_CONNECTIONS = 4  # S3连接池大小，个
S3_UPLOAD_MAX_CONCURRENCY = 1  # S3单文件并发上传数，个
S3_MULTIPART_THRESHOLD_BYTES = 4 * 1024 * 1024  # S3分片上传阈值，字节
S3_MULTIPART_CHUNKSIZE_BYTES = 4 * 1024 * 1024  # S3分片大小，字节
S3_USE_THREADS = False  # S3并发传输开关，开关
S3_TRANSFER_RETRY_TIMES = 5  # S3传输重试次数，次
S3_TRANSFER_RETRY_INTERVAL_SECONDS = 3  # S3传输重试间隔，秒
S3_UPLOAD_POOL_WORKERS = 3  # S3上传池工作线程数，个

WS_TIMEOUT_SECONDS = 10  # WS连接超时，秒
WS_RECV_TIMEOUT_SECONDS = 30  # WS接收超时，秒
WS_RECONNECT_INTERVAL_SECONDS = 2  # WS重连间隔，秒
WS_STATUS_INTERVAL_SECONDS = 1  # WS状态输出间隔，秒
WS_PING_INTERVAL_SECONDS = 10  # WS心跳间隔，秒
WS_STANDBY_BUFFER_MAX_LINES = 100000  # WS待切换缓存每文件最大行数，行
OLD_LAUNCHER_CHECK_INTERVAL_SECONDS = 0.5  # 旧版本启动器检测间隔，秒

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


def parse_symbols(text: str) -> list:
    """按逗号解析交易对文本。"""
    return [item.strip() for item in text.split(",") if item.strip()]


def parse_bybit_symbols(text: str) -> list:
    """兼容旧代码的交易对解析函数。"""
    return parse_symbols(text)
