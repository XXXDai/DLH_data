from collections import deque
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError, ConnectTimeoutError, ConnectionClosedError, EndpointConnectionError, NoCredentialsError, PartialCredentialsError, ReadTimeoutError
import json
import os
import queue
import resource
import shutil
import signal
import socket
import sys
import threading
import time
import traceback
import websocket


def normalize_s3_prefix(text: str) -> str:
    """规范化S3目录前缀。"""
    return text.strip().strip("/")


S3_BUCKET_NAME = os.getenv("DLH_S3_BUCKET_NAME", "main-ai-ext").strip()  # S3桶名称，字符串
S3_PREFIX = normalize_s3_prefix(os.getenv("DLH_S3_PREFIX", "dlh/data/dylan"))  # S3目录前缀，路径
DATA_STORAGE_MODE = "local"  # 数据存储模式，可选local或s3，字符串

DATA_ROOT = Path("data")  # 数据根目录，路径
KEEP_SUBDIRS = ("src", "dwd", "dws")  # 本地重置保留的一级目录列表，个数

INSTRUMENTS_TIMEOUT_SECONDS = 10  # 交易对接口超时，秒
HTTP_TIMEOUT_SECONDS = 10  # HTTP请求超时，秒
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
WS_WRITE_BUFFER_LINES = 256  # WS单文件写入缓冲行数阈值，行
WS_WRITE_BUFFER_BYTES = 1024 * 1024  # WS单文件写入缓冲字节阈值，字节
WS_WRITE_BUFFER_INTERVAL_SECONDS = 1  # WS单文件写入缓冲刷新间隔，秒
DELIVERY_REFRESH_SECONDS = 15 * 60  # 动态合约刷新间隔，秒
ORDERBOOK_DEPTH_FUTURE = 200  # 期货订单簿深度，档位
ORDERBOOK_DEPTH_SPOT = 50  # 现货订单簿深度，档位
KEEP_ORDERBOOK_LEVELS = 2000  # 内存保留盘口层数，档位
SUMMARY_INTERVAL_SECONDS = 60  # 状态摘要输出间隔，秒
LOG_LINES_PER_TASK = 50  # 每任务控制台日志缓存行数，行
MAX_LOG_LINE_CHARS = 2000  # 单行日志最大长度，字符

ERROR_LOG_PATH = Path("logs/error.log")  # 错误日志路径，路径
ERROR_LOG_KEYWORDS = ("错误", "异常", "Traceback", "Exception")  # 错误关键词列表，个数
ERROR_LOG_EXCLUDE_KEYWORDS = ("HTTP错误404", "HTTP Error 404")  # 错误排除关键词列表，个数

BYBIT_FUTURE_WS_URL = "wss://stream.bybit.com/v5/public/linear"  # Bybit期货WS地址，字符串
BYBIT_SPOT_WS_URL = "wss://stream.bybit.com/v5/public/spot"  # Bybit现货WS地址，字符串
BINANCE_FUTURE_WS_URL = "wss://fstream.binance.com/ws/{stream}"  # Binance期货WS地址模板，字符串
BINANCE_SPOT_WS_URL = "wss://stream.binance.com:9443/ws/{stream}"  # Binance现货WS地址模板，字符串
BITGET_WS_URL = "wss://ws.bitget.com/v2/ws/public"  # Bitget公共WS地址，字符串
OKX_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"  # OKX公共WS地址，字符串
OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"  # OKX合约接口地址，字符串

PRIMARY_ROLE = "primary"  # 主连接角色标识，字符串
BACKUP_ROLE = "backup"  # 备连接角色标识，字符串

EXCHANGE_ENABLED = {
    "bybit": False,  # Bybit总开关，开关
    "binance": True,  # Binance总开关，开关
    "bitget": True,  # Bitget总开关，开关
    "okx": True,  # OKX总开关，开关
}  # 交易所总开关映射，映射
SPOT_SYMBOLS = {
    "bybit": [],  # Bybit现货交易对列表，个数
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],  # Binance现货交易对列表，个数
    "bitget": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],  # Bitget现货交易对列表，个数
    "okx": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],  # OKX现货交易对列表，个数
}  # 现货交易对映射，映射
FUTURE_PERPETUAL_SYMBOLS = {
    "bybit": [],  # Bybit永续交易对列表，个数
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],  # Binance永续交易对列表，个数
    "bitget": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],  # Bitget永续交易对列表，个数
    "okx": ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"],  # OKX永续交易对列表，个数
}  # 永续交易对映射，映射
FUTURE_DELIVERY_FAMILIES = {
    "bybit": [],  # Bybit交割合约家族列表，个数
    "binance": [],  # Binance交割合约家族列表，个数
    "bitget": [],  # Bitget交割合约家族列表，个数
    "okx": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],  # OKX交割合约家族列表，个数
}  # 交割合约家族映射，映射
DATASET_SUPPORT = {
    "D10002-4": {
        "bybit": False,  # Bybit期货实时订单簿支持开关，开关
        "binance": True,  # Binance期货实时订单簿支持开关，开关
        "bitget": True,  # Bitget期货实时订单簿支持开关，开关
        "okx": True,  # OKX期货实时订单簿支持开关，开关
    },  # D10002-4支持矩阵，映射
    "D10006-8": {
        "bybit": False,  # Bybit现货实时订单簿支持开关，开关
        "binance": True,  # Binance现货实时订单簿支持开关，开关
        "bitget": True,  # Bitget现货实时订单簿支持开关，开关
        "okx": True,  # OKX现货实时订单簿支持开关，开关
    },  # D10006-8支持矩阵，映射
}  # 实时数据集支持矩阵，映射

EXIT_REQUESTED = threading.Event()  # 全局退出事件，事件
CONSOLE_WRITE_LOCK = threading.Lock()  # 控制台输出锁，锁
ERROR_LOG_LOCK = threading.Lock()  # 错误日志输出锁，锁
UPLOAD_QUEUE = queue.Queue()  # S3上传任务队列，队列
UPLOAD_QUEUE_LOCK = threading.Lock()  # S3上传队列锁，锁
UPLOAD_PENDING_PATHS = set()  # S3待上传路径集合，个数
UPLOAD_WORKER_THREADS = []  # S3上传工作线程列表，个数
UPLOAD_ACTIVE_TASKS = {}  # S3活跃上传任务映射，映射
UPLOAD_STATUS_LOCK = threading.Lock()  # S3上传状态锁，锁
UPLOAD_STARTUP_SYNC_DONE = False  # S3启动扫描是否完成，开关

LOG_LINES = {
    "D10002-4": deque(maxlen=LOG_LINES_PER_TASK),  # 期货任务日志缓存，行
    "D10006-8": deque(maxlen=LOG_LINES_PER_TASK),  # 现货任务日志缓存，行
    "launcher_wss": deque(maxlen=LOG_LINES_PER_TASK),  # 启动器日志缓存，行
    "upload": deque(maxlen=LOG_LINES_PER_TASK),  # 上传任务日志缓存，行
}  # 控制台日志缓存映射，映射
STATUS_COUNTS = {
    "D10002-4": {},  # 期货状态映射，映射
    "D10006-8": {},  # 现货状态映射，映射
}  # 任务状态映射，映射
STATUS_TIMES = {
    "D10002-4": 0.0,  # 期货最近更新时间戳，秒
    "D10006-8": 0.0,  # 现货最近更新时间戳，秒
    "launcher_wss": 0.0,  # 启动器最近更新时间戳，秒
    "upload": 0.0,  # 上传最近更新时间戳，秒
}  # 任务更新时间映射，映射
RUNTIME_OBSERVE_CACHE = {"ts": 0.0, "text": ""}  # 运行时观测缓存，映射


class NetworkRequestError(RuntimeError):
    """网络请求错误。"""


class UploadProgressTracker:
    """记录单个上传任务的进度。"""

    def __init__(self, file_path: Path):
        """初始化上传进度跟踪器。"""
        self.file_path = file_path.resolve()
        self.worker_name = threading.current_thread().name
        self.total_bytes = self.file_path.stat().st_size if self.file_path.exists() else 0
        self.uploaded_bytes = 0
        self.started_at = time.time()
        self.updated_at = self.started_at
        with UPLOAD_STATUS_LOCK:
            UPLOAD_ACTIVE_TASKS[self.worker_name] = {
                "file_path": str(self.file_path),
                "file_name": self.file_path.name,
                "uploaded_bytes": self.uploaded_bytes,
                "total_bytes": self.total_bytes,
                "started_at": self.started_at,
                "updated_at": self.updated_at,
            }

    def __call__(self, bytes_amount: int) -> None:
        """接收单次上传进度回调。"""
        self.uploaded_bytes += bytes_amount
        self.updated_at = time.time()
        with UPLOAD_STATUS_LOCK:
            UPLOAD_ACTIVE_TASKS[self.worker_name] = {
                "file_path": str(self.file_path),
                "file_name": self.file_path.name,
                "uploaded_bytes": self.uploaded_bytes,
                "total_bytes": self.total_bytes,
                "started_at": self.started_at,
                "updated_at": self.updated_at,
            }

    def finish(self) -> None:
        """结束当前上传任务跟踪。"""
        with UPLOAD_STATUS_LOCK:
            UPLOAD_ACTIVE_TASKS.pop(self.worker_name, None)


class Task:
    """表示一个WSS任务。"""

    def __init__(self, task_id: str, name: str, market: str):
        """初始化任务对象。"""
        self.task_id = task_id
        self.name = name
        self.market = market
        self.thread = None

    def start(self) -> None:
        """启动任务线程。"""
        self.thread = threading.Thread(target=run_market_ws, args=(self.market,), name=self.task_id, daemon=True)
        self.thread.start()

    def is_running(self) -> bool:
        """判断任务线程是否仍在运行。"""
        return self.thread is not None and self.thread.is_alive()


def normalize_symbol_list(symbols: list[str]) -> list[str]:
    """对交易对列表去重并排序。"""
    return sorted(set(symbols))


def apply_storage_mode_from_argv() -> None:
    """根据启动参数设置存储模式。"""
    global DATA_STORAGE_MODE
    DATA_STORAGE_MODE = "s3" if "-s3" in sys.argv else "local"


def has_remove_flag() -> bool:
    """判断是否启用清理启动参数。"""
    return "-rm" in sys.argv


def is_s3_storage_mode() -> bool:
    """判断当前是否为S3模式。"""
    return DATA_STORAGE_MODE == "s3"


apply_storage_mode_from_argv()


def market_task_id(market: str | None) -> str:
    """返回市场对应的任务标识。"""
    if market == "future":
        return "D10002-4"
    if market == "spot":
        return "D10006-8"
    return "launcher_wss"


def sanitize_log_text(text: str) -> str:
    """清洗日志文本中的控制字符。"""
    text = text.replace("\t", " ")
    out = []
    idx = 0
    while idx < len(text):
        ch = text[idx]
        if ch == "\x1b" and idx + 1 < len(text) and text[idx + 1] == "[":
            idx += 2
            while idx < len(text):
                tail = text[idx]
                if "@" <= tail <= "~":
                    idx += 1
                    break
                idx += 1
            continue
        if ord(ch) < 32 and ch != "\n":
            idx += 1
            continue
        out.append(ch)
        idx += 1
    cleaned = "".join(out)
    if len(cleaned) > MAX_LOG_LINE_CHARS:
        return cleaned[:MAX_LOG_LINE_CHARS]
    return cleaned


def write_console_line(text: str) -> None:
    """线程安全输出单行控制台日志。"""
    with CONSOLE_WRITE_LOCK:
        sys.__stdout__.write(f"{sanitize_log_text(text)}\n")
        sys.__stdout__.flush()


def contains_error_keyword(text: str) -> bool:
    """判断文本是否属于错误日志。"""
    lowered = text.lower()
    for keyword in ERROR_LOG_EXCLUDE_KEYWORDS:
        if keyword.lower() in lowered:
            return False
    for keyword in ERROR_LOG_KEYWORDS:
        if keyword.lower() in lowered:
            return True
    return False


def write_error_line(source: str, text: str) -> None:
    """写入本地错误日志。"""
    if not contains_error_keyword(text):
        return
    ERROR_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    with ERROR_LOG_LOCK:
        with ERROR_LOG_PATH.open("a", encoding="utf-8") as file_obj:
            file_obj.write(f"{timestamp} [{source}] {sanitize_log_text(text)}\n")


def log(message: str, market: str | None = None) -> None:
    """输出运行日志并按需记录错误。"""
    task_id = market_task_id(market)
    LOG_LINES[task_id].append(message)
    STATUS_TIMES[task_id] = time.time()
    write_console_line(f"[{task_id}] {message}")
    write_error_line(task_id, message)


def log_upload(message: str) -> None:
    """输出上传相关日志。"""
    LOG_LINES["upload"].append(message)
    STATUS_TIMES["upload"] = time.time()
    write_console_line(f"[upload] {message}")
    write_error_line("upload", message)


def install_exception_hooks() -> None:
    """安装进程异常日志钩子。"""

    def handle_exception(exc_type, exc_value, exc_traceback) -> None:
        """处理主线程未捕获异常。"""
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        for line in "".join(lines).splitlines():
            write_error_line("launcher_wss", line)
            write_console_line(f"[launcher_wss] {line}")

    def handle_thread_exception(args) -> None:
        """处理子线程未捕获异常。"""
        lines = traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback)
        task_id = str(args.thread.name or "thread")
        for line in "".join(lines).splitlines():
            write_error_line(task_id, line)
            write_console_line(f"[{task_id}] {line}")

    sys.excepthook = handle_exception
    threading.excepthook = handle_thread_exception


def handle_sigint(_signum, _frame) -> None:
    """统一处理Ctrl+C退出请求。"""
    EXIT_REQUESTED.set()


def restore_stdio() -> None:
    """恢复标准输出与错误输出。"""
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


def clear_local_data(root: Path) -> None:
    """清理本地数据目录。"""
    root.mkdir(parents=True, exist_ok=True)
    for subdir_name in KEEP_SUBDIRS:
        target = root / subdir_name
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)


def format_speed_text(speed_bytes_per_second: float) -> str:
    """格式化上传速度文本。"""
    if speed_bytes_per_second <= 0:
        return "0 B/s"
    units = ["B/s", "KB/s", "MB/s", "GB/s"]
    value = float(speed_bytes_per_second)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if value >= 100:
        return f"{value:.0f} {units[unit_index]}"
    if value >= 10:
        return f"{value:.1f} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def format_bytes_text(byte_count: int) -> str:
    """格式化字节数文本。"""
    if byte_count <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(byte_count)
    unit_index = 0
    while value >= 1024 and unit_index < len(units) - 1:
        value /= 1024.0
        unit_index += 1
    if value >= 100:
        return f"{value:.0f} {units[unit_index]}"
    if value >= 10:
        return f"{value:.1f} {units[unit_index]}"
    return f"{value:.2f} {units[unit_index]}"


def read_proc_status_value_bytes(field_name: str) -> int:
    """读取当前进程状态文件中的内存字段。"""
    proc_status_path = Path("/proc/self/status")
    if not proc_status_path.exists():
        return 0
    for line in proc_status_path.read_text(encoding="utf-8").splitlines():
        if not line.startswith(f"{field_name}:"):
            continue
        parts = line.split()
        if len(parts) < 2 or not parts[1].isdigit():
            return 0
        value = int(parts[1])
        unit = parts[2] if len(parts) >= 3 else ""
        if unit == "kB":
            return value * 1024
        return value
    return 0


def read_process_rss_bytes() -> int:
    """读取当前进程常驻内存大小。"""
    rss_bytes = read_proc_status_value_bytes("VmRSS")
    if rss_bytes > 0:
        return rss_bytes
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def read_process_peak_rss_bytes() -> int:
    """读取当前进程峰值常驻内存大小。"""
    peak_bytes = read_proc_status_value_bytes("VmHWM")
    if peak_bytes > 0:
        return peak_bytes
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def build_data_dir(prefix: str, dataset_name: str) -> Path:
    """构造数据目录。"""
    return DATA_ROOT / prefix / dataset_name


def build_orderbook_rt_dir(exchange: str, market: str, stage: str) -> Path:
    """构造实时订单簿目录。"""
    return build_data_dir("src", f"{exchange}_{market}_orderbook_{stage}")


def build_orderbook_rt_tag(exchange: str, market: str, stage: str) -> str:
    """构造实时订单簿标签。"""
    return f"{exchange}_{market}_orderbook_{stage}"


def ensure_parent(path: Path) -> None:
    """确保目标父目录存在。"""
    path.parent.mkdir(parents=True, exist_ok=True)


def build_s3_key(file_path: Path) -> str | None:
    """构造本地文件对应的S3对象键。"""
    absolute_path = file_path.resolve()
    data_root = DATA_ROOT.resolve()
    if not absolute_path.is_relative_to(data_root):
        return None
    relative_path = absolute_path.relative_to(data_root)
    return f"{S3_PREFIX}/{relative_path.as_posix()}"


@lru_cache(maxsize=1)
def get_s3_client():
    """构造S3客户端。"""
    return boto3.client(
        "s3",
        config=Config(
            connect_timeout=S3_CONNECT_TIMEOUT_SECONDS,
            read_timeout=S3_READ_TIMEOUT_SECONDS,
            retries={"max_attempts": S3_MAX_ATTEMPTS, "mode": "standard"},
            max_pool_connections=S3_MAX_POOL_CONNECTIONS,
        ),
    )


@lru_cache(maxsize=1)
def get_s3_transfer_config() -> TransferConfig:
    """构造S3传输配置。"""
    return TransferConfig(
        multipart_threshold=S3_MULTIPART_THRESHOLD_BYTES,
        max_concurrency=S3_UPLOAD_MAX_CONCURRENCY,
        multipart_chunksize=S3_MULTIPART_CHUNKSIZE_BYTES,
        use_threads=S3_USE_THREADS,
    )


def upload_file_to_s3_blocking(file_path: Path) -> None:
    """同步上传单个本地文件到S3。"""
    if not is_s3_storage_mode():
        return
    if not file_path.exists() or not file_path.is_file():
        return
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return
    for attempt in range(1, S3_TRANSFER_RETRY_TIMES + 1):
        tracker = UploadProgressTracker(file_path)
        try:
            get_s3_client().upload_file(
                str(file_path),
                S3_BUCKET_NAME,
                s3_key,
                Config=get_s3_transfer_config(),
                Callback=tracker,
            )
            if file_path.exists():
                file_path.unlink()
            return
        except NoCredentialsError as exc:
            raise RuntimeError("S3上传失败: 缺少凭证") from exc
        except PartialCredentialsError as exc:
            raise RuntimeError("S3上传失败: 凭证不完整") from exc
        except ClientError as exc:
            raise RuntimeError(f"S3上传失败: {exc.response.get('Error', {}).get('Code', '未知错误')}") from exc
        except (ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError, ConnectionClosedError) as exc:
            if attempt >= S3_TRANSFER_RETRY_TIMES:
                log_upload(f"S3上传失败，已保留本地文件稍后重试: {file_path} | {exc}")
                return
            log_upload(f"S3上传重试 {attempt}/{S3_TRANSFER_RETRY_TIMES}: {file_path} | {exc}")
            time.sleep(S3_TRANSFER_RETRY_INTERVAL_SECONDS)
        finally:
            tracker.finish()


def upload_worker_loop() -> None:
    """循环处理S3上传队列。"""
    while True:
        file_path = UPLOAD_QUEUE.get()
        try:
            upload_file_to_s3_blocking(file_path)
        finally:
            with UPLOAD_QUEUE_LOCK:
                UPLOAD_PENDING_PATHS.discard(str(file_path.resolve()))
            UPLOAD_QUEUE.task_done()


def ensure_upload_workers_started() -> None:
    """确保S3上传线程池已启动。"""
    alive_threads = [worker for worker in UPLOAD_WORKER_THREADS if worker.is_alive()]
    UPLOAD_WORKER_THREADS[:] = alive_threads
    for index in range(len(alive_threads), S3_UPLOAD_POOL_WORKERS):
        worker = threading.Thread(target=upload_worker_loop, name=f"s3-upload-{index + 1}", daemon=True)
        worker.start()
        UPLOAD_WORKER_THREADS.append(worker)
    sync_local_files_to_s3_on_startup()


def enqueue_file_for_s3_upload(file_path: Path) -> None:
    """将本地文件加入S3上传队列。"""
    if not is_s3_storage_mode():
        return
    if not file_path.exists() or not file_path.is_file():
        return
    file_key = str(file_path.resolve())
    with UPLOAD_QUEUE_LOCK:
        if file_key in UPLOAD_PENDING_PATHS:
            return
        UPLOAD_PENDING_PATHS.add(file_key)
    UPLOAD_QUEUE.put(file_path.resolve())


def list_all_s3_keys_under_data_root() -> set[str]:
    """列出数据根目录下所有S3对象键。"""
    prefix = f"{S3_PREFIX}/"
    keys = set()
    try:
        paginator = get_s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
            for item in page.get("Contents", []):
                key = str(item.get("Key") or "")
                if key and not key.endswith("/"):
                    keys.add(key)
        return keys
    except NoCredentialsError as exc:
        raise RuntimeError("S3检查失败: 缺少凭证") from exc
    except PartialCredentialsError as exc:
        raise RuntimeError("S3检查失败: 凭证不完整") from exc
    except ConnectTimeoutError as exc:
        raise RuntimeError("S3检查失败: 连接超时") from exc
    except ReadTimeoutError as exc:
        raise RuntimeError("S3检查失败: 读取超时") from exc
    except EndpointConnectionError as exc:
        raise RuntimeError("S3检查失败: 无法连接S3端点") from exc
    except ClientError as exc:
        raise RuntimeError(f"S3检查失败: {exc.response.get('Error', {}).get('Code', '未知错误')}") from exc


def sync_local_files_to_s3_on_startup() -> None:
    """启动时按S3现状补传并清理本地文件。"""
    global UPLOAD_STARTUP_SYNC_DONE
    if UPLOAD_STARTUP_SYNC_DONE:
        return
    if not is_s3_storage_mode():
        UPLOAD_STARTUP_SYNC_DONE = True
        return
    if not DATA_ROOT.exists():
        UPLOAD_STARTUP_SYNC_DONE = True
        return
    log_upload("S3启动扫描开始")
    local_files = sorted([path for path in DATA_ROOT.rglob("*") if path.is_file()])
    existing_s3_keys = list_all_s3_keys_under_data_root()
    queued_count = 0
    deleted_count = 0
    for file_path in local_files:
        if file_path.name.endswith(".part"):
            continue
        s3_key = build_s3_key(file_path)
        if s3_key and s3_key in existing_s3_keys:
            file_path.unlink()
            deleted_count += 1
            continue
        file_key = str(file_path.resolve())
        with UPLOAD_QUEUE_LOCK:
            if file_key in UPLOAD_PENDING_PATHS:
                continue
            UPLOAD_PENDING_PATHS.add(file_key)
        UPLOAD_QUEUE.put(file_path.resolve())
        queued_count += 1
    UPLOAD_STARTUP_SYNC_DONE = True
    log_upload(f"S3启动扫描完成: 已入队 {queued_count} | 已清理 {deleted_count}")


def get_upload_pool_snapshot() -> dict:
    """返回上传池当前状态快照。"""
    with UPLOAD_STATUS_LOCK:
        active_tasks = list(UPLOAD_ACTIVE_TASKS.values())
    with UPLOAD_QUEUE_LOCK:
        pending_count = UPLOAD_QUEUE.qsize()
        pending_file_names = []
        for file_path in list(UPLOAD_QUEUE.queue)[:8]:
            pending_file_names.append(Path(file_path).name)
    alive_worker_count = sum(1 for worker in UPLOAD_WORKER_THREADS if worker.is_alive())
    speed_bytes_per_second = 0.0
    file_names = []
    for item in active_tasks:
        file_names.append(item["file_name"])
        elapsed_seconds = max(0.001, time.time() - float(item["started_at"]))
        speed_bytes_per_second += float(item["uploaded_bytes"]) / elapsed_seconds
    return {
        "enabled": is_s3_storage_mode(),
        "workers": S3_UPLOAD_POOL_WORKERS,
        "alive_worker_count": alive_worker_count,
        "active_count": len(active_tasks),
        "pending_count": pending_count,
        "speed_bytes_per_second": speed_bytes_per_second,
        "file_names": file_names,
        "pending_file_names": pending_file_names,
    }


def request_json(url: str) -> dict:
    """请求JSON接口并返回字典。"""
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=INSTRUMENTS_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise NetworkRequestError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise NetworkRequestError("接口请求失败: 网络错误") from exc
    except TimeoutError as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc
    except socket.timeout as exc:
        raise NetworkRequestError("接口请求失败: 超时") from exc


def list_exchanges() -> list[str]:
    """返回全部启用交易所。"""
    return [exchange for exchange, enabled in EXCHANGE_ENABLED.items() if enabled]


def is_supported(dataset_id: str, exchange: str) -> bool:
    """判断指定数据集是否支持该交易所。"""
    return bool(EXCHANGE_ENABLED.get(exchange, False) and DATASET_SUPPORT.get(dataset_id, {}).get(exchange))


def get_spot_symbols(exchange: str) -> list[str]:
    """返回指定交易所现货交易对。"""
    return list(SPOT_SYMBOLS.get(exchange, []))


def get_future_symbols(exchange: str) -> list[str]:
    """返回指定交易所永续交易对。"""
    return list(FUTURE_PERPETUAL_SYMBOLS.get(exchange, []))


def get_delivery_families(exchange: str) -> list[str]:
    """返回指定交易所交割合约家族。"""
    return list(FUTURE_DELIVERY_FAMILIES.get(exchange, []))


def get_status_key(exchange: str, market: str, symbol: str) -> str:
    """返回统一状态键。"""
    return f"{exchange}/{market}/{symbol}"


def build_dirs(exchange: str, market: str) -> tuple[Path, Path, Path, str, str, str]:
    """构造实时订单簿输出目录。"""
    rt_dir = build_orderbook_rt_dir(exchange, market, "rt")
    rt_ss_dir = build_orderbook_rt_dir(exchange, market, "rt_ss")
    rt_ss_1s_dir = build_orderbook_rt_dir(exchange, market, "rt_ss_1s")
    rt_tag = build_orderbook_rt_tag(exchange, market, "rt")
    rt_ss_tag = build_orderbook_rt_tag(exchange, market, "rt_ss")
    rt_ss_1s_tag = build_orderbook_rt_tag(exchange, market, "rt_ss_1s")
    return rt_dir, rt_ss_dir, rt_ss_1s_dir, rt_tag, rt_ss_tag, rt_ss_1s_tag


def build_file_path(base_dir: Path, symbol: str, hour_str: str, tag: str) -> Path:
    """构造实时小时文件路径。"""
    file_name = f"{symbol}-{tag}-{hour_str}.json"
    return base_dir / symbol / hour_str / file_name


def hour_str_from_ms(ts_ms: int) -> str:
    """将毫秒时间戳转换为小时分区。"""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%Y%m%d%H")


def ensure_writer(base_dir: Path, symbol: str, hour_str: str, tag: str, writer):
    """确保当前小时文件写入器可用。"""
    target_path = build_file_path(base_dir, symbol, hour_str, tag)
    if writer and writer["path"] == target_path:
        return writer
    if writer:
        close_writer(writer)
    ensure_parent(target_path)
    return {
        "path": target_path,
        "file": target_path.open("a", encoding="utf-8"),
        "buffer": [],
        "buffer_bytes": 0,
        "last_flush": time.monotonic(),
    }


def flush_writer_buffer(writer) -> None:
    """将写入缓冲刷到磁盘。"""
    if not writer or not writer["buffer"]:
        return
    writer["file"].write("".join(writer["buffer"]))
    writer["file"].flush()
    writer["buffer"].clear()
    writer["buffer_bytes"] = 0
    writer["last_flush"] = time.monotonic()


def close_writer(writer) -> None:
    """关闭单个写入器并触发上传。"""
    if not writer:
        return
    flush_writer_buffer(writer)
    writer["file"].close()
    enqueue_file_for_s3_upload(writer["path"])


def close_writers(rt_writer, rt_ss_writer, rt_ss_1s_writer) -> tuple[None, None, None]:
    """关闭三类订单簿写入器。"""
    close_writer(rt_writer)
    close_writer(rt_ss_writer)
    close_writer(rt_ss_1s_writer)
    return None, None, None


def format_number(value: float) -> str:
    """将数字格式化为紧凑字符串。"""
    return format(value, "f").rstrip("0").rstrip(".") if "." in format(value, "f") else str(value)


def replace_orderbook(orderbook: dict, bids: list, asks: list) -> None:
    """用完整快照替换当前盘口。"""
    orderbook["bids"] = {float(price): float(size) for price, size in bids}
    orderbook["asks"] = {float(price): float(size) for price, size in asks}


def apply_orderbook_delta(orderbook: dict, bids: list, asks: list) -> None:
    """将增量消息应用到当前盘口。"""
    for price, size in bids:
        price_value = float(price)
        size_value = float(size)
        if size_value == 0:
            orderbook["bids"].pop(price_value, None)
        else:
            orderbook["bids"][price_value] = size_value
    for price, size in asks:
        price_value = float(price)
        size_value = float(size)
        if size_value == 0:
            orderbook["asks"].pop(price_value, None)
        else:
            orderbook["asks"][price_value] = size_value


def trim_orderbook(orderbook: dict) -> None:
    """裁剪内存盘口层数。"""
    if len(orderbook["bids"]) > KEEP_ORDERBOOK_LEVELS:
        bids_sorted = sorted(orderbook["bids"].items(), key=lambda item: item[0], reverse=True)[:KEEP_ORDERBOOK_LEVELS]
        orderbook["bids"] = dict(bids_sorted)
    if len(orderbook["asks"]) > KEEP_ORDERBOOK_LEVELS:
        asks_sorted = sorted(orderbook["asks"].items(), key=lambda item: item[0])[:KEEP_ORDERBOOK_LEVELS]
        orderbook["asks"] = dict(asks_sorted)


def build_snapshot(
    symbol: str,
    orderbook: dict,
    update_type: str,
    ts_ms: int,
    cts_ms: int,
    collect_ts: int,
    update_id: int | None,
    seq: int | None,
    depth: int,
) -> dict:
    """构造统一快照结构。"""
    bids_sorted = sorted(orderbook["bids"].items(), key=lambda item: item[0], reverse=True)[:depth]
    asks_sorted = sorted(orderbook["asks"].items(), key=lambda item: item[0])[:depth]
    best_bid = bids_sorted[0][0] if bids_sorted else None
    best_ask = asks_sorted[0][0] if asks_sorted else None
    return {
        "symbol": symbol,
        "update_type": update_type,
        "ts": ts_ms,
        "cts": cts_ms,
        "collect_ts": collect_ts,
        "update_id": update_id,
        "seq": seq,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "bid_depth": len(bids_sorted),
        "ask_depth": len(asks_sorted),
        "bids": [[format_number(price), format_number(size)] for price, size in bids_sorted],
        "asks": [[format_number(price), format_number(size)] for price, size in asks_sorted],
    }


def list_okx_delivery_symbols() -> list[str]:
    """拉取OKX当前可订阅的交割合约。"""
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    symbols = set()
    for family in get_delivery_families("okx"):
        payload = request_json(f"{OKX_INSTRUMENTS_URL}?{urlencode({'instType': 'FUTURES', 'instFamily': family})}")
        if payload.get("code") != "0":
            raise NetworkRequestError(f"接口返回错误: {payload.get('msg')}")
        for item in payload.get("data", []):
            inst_id = item.get("instId", "")
            state = item.get("state", "")
            exp_time = int(item.get("expTime", "0") or 0)
            if state not in {"live", "preopen"}:
                continue
            if exp_time and exp_time < now_ms:
                continue
            symbols.add(inst_id)
    return sorted(symbols)


def resolve_symbols(exchange: str, market: str) -> list[str]:
    """解析某个交易所当前应运行的交易对。"""
    if market == "spot":
        return normalize_symbol_list(get_spot_symbols(exchange))
    symbols = set(get_future_symbols(exchange))
    if exchange == "okx":
        symbols.update(list_okx_delivery_symbols())
    return sorted(symbols)


def connect_ws(url: str) -> websocket.WebSocket:
    """建立WebSocket连接。"""
    ws = websocket.create_connection(url, timeout=WS_TIMEOUT_SECONDS)
    ws.settimeout(WS_RECV_TIMEOUT_SECONDS)
    return ws


def build_ws_url(exchange: str, market: str, symbol: str) -> str:
    """构造指定交易所的WebSocket地址。"""
    if exchange == "bybit":
        return BYBIT_FUTURE_WS_URL if market == "future" else BYBIT_SPOT_WS_URL
    if exchange == "binance":
        stream = f"{symbol.lower()}@depth20@100ms"
        return (BINANCE_FUTURE_WS_URL if market == "future" else BINANCE_SPOT_WS_URL).format(stream=stream)
    if exchange == "bitget":
        return BITGET_WS_URL
    if exchange == "okx":
        return OKX_WS_URL
    raise RuntimeError(f"未支持的交易所: {exchange}")


def build_ws_urls(exchange: str, market: str, symbol: str) -> list[str]:
    """构造主备WebSocket地址列表。"""
    primary_url = build_ws_url(exchange, market, symbol)
    return [primary_url, primary_url]


def send_subscribe(ws: websocket.WebSocket, exchange: str, market: str, symbol: str, depth: int) -> None:
    """发送订阅请求。"""
    if exchange == "bitget":
        inst_type = "USDT-FUTURES" if market == "future" else "SPOT"
        payload = {"op": "subscribe", "args": [{"instType": inst_type, "channel": "books", "instId": symbol}]}
        ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return
    if exchange == "okx":
        payload = {"op": "subscribe", "args": [{"channel": "books", "instId": symbol}]}
        ws.send(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        return


def ping_payload(exchange: str) -> str | None:
    """返回指定交易所的心跳内容。"""
    if exchange in {"bitget", "okx"}:
        return "ping"
    return None


def build_session_state() -> dict:
    """构造主备连接共享状态。"""
    return {
        "lock": threading.Lock(),
        "active_role": PRIMARY_ROLE,
        "connected": {PRIMARY_ROLE: False, BACKUP_ROLE: False},
        "recv_count": {PRIMARY_ROLE: 0, BACKUP_ROLE: 0},
        "status_text": {PRIMARY_ROLE: "准备连接", BACKUP_ROLE: "准备连接"},
    }


def role_label(role: str) -> str:
    """返回连接角色显示名称。"""
    return "主" if role == PRIMARY_ROLE else "备"


def other_role(role: str) -> str:
    """返回另一个连接角色。"""
    return BACKUP_ROLE if role == PRIMARY_ROLE else PRIMARY_ROLE


def summarize_role_status(connected: bool, status_text: str) -> str:
    """将单个角色状态压缩为短文本。"""
    if connected:
        return "在线"
    if status_text in {"准备连接", "已连接 0"}:
        return "连接中"
    if status_text in {"连接异常", "连接超时", "网络错误", "连接关闭"}:
        return "重连中"
    if status_text == "已停止":
        return "已停"
    return status_text


def build_ws_status_text(state: dict) -> tuple[int, str]:
    """构造主备连接汇总状态文本。"""
    active_role = state["active_role"]
    active_count = state["recv_count"][active_role]
    primary_connected = state["connected"][PRIMARY_ROLE]
    backup_connected = state["connected"][BACKUP_ROLE]
    primary_short = summarize_role_status(primary_connected, state["status_text"][PRIMARY_ROLE])
    backup_short = summarize_role_status(backup_connected, state["status_text"][BACKUP_ROLE])
    if primary_connected and backup_connected:
        role_summary = "主活 备连" if active_role == PRIMARY_ROLE else "备活 主连"
    elif primary_connected:
        role_summary = "主活 备断" if active_role == PRIMARY_ROLE else "已切主"
    elif backup_connected:
        role_summary = "已切备" if active_role == BACKUP_ROLE else "备活 主断"
    elif primary_short == "连接中" and backup_short == "连接中":
        role_summary = "首连中"
    else:
        role_summary = "双断重连中"
    online_flag = 1 if state["connected"][active_role] else 0
    return online_flag, f"{role_summary} | 主:{primary_short} 备:{backup_short} | 消息:{active_count}"


def status_update(exchange: str, market: str, symbol: str, value) -> None:
    """更新统一状态键的状态值。"""
    task_id = market_task_id(market)
    key = get_status_key(exchange, market, symbol)
    if value is None:
        STATUS_COUNTS[task_id].pop(key, None)
    else:
        STATUS_COUNTS[task_id][key] = value
    STATUS_TIMES[task_id] = time.time()


def update_shared_status(
    state: dict,
    exchange: str,
    market: str,
    symbol: str,
    role: str,
    connected: bool | None = None,
    status_text: str | None = None,
    recv_count: int | None = None,
) -> None:
    """更新主备连接共享状态。"""
    with state["lock"]:
        if connected is not None:
            state["connected"][role] = connected
        if status_text is not None:
            state["status_text"][role] = status_text
        if recv_count is not None:
            state["recv_count"][role] = recv_count
        online_flag, status_text_value = build_ws_status_text(state)
    status_update(exchange, market, symbol, (online_flag, status_text_value))


def switch_active_role(state: dict, exchange: str, market: str, symbol: str, failed_role: str) -> None:
    """在主连接失效时切换到另一个角色。"""
    next_role = other_role(failed_role)
    switched = False
    with state["lock"]:
        if state["active_role"] == failed_role and state["connected"][next_role]:
            state["active_role"] = next_role
            switched = True
    if switched:
        log(f"{exchange} {market} {symbol} 主备切换到{role_label(next_role)}连接", market)
    update_shared_status(state, exchange, market, symbol, failed_role)


def normalize_binance_raw(message: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将Binance原始消息归一化为Bybit样式。"""
    ts_ms = int(message.get("E", "0") or 0)
    cts_ms = int(message.get("T", ts_ms) or ts_ms)
    update_id = int(message.get("u", "0") or 0)
    return {
        "topic": f"orderbook.{depth}.{symbol}",
        "symbol": symbol,
        "type": "snapshot",
        "ts": ts_ms,
        "cts": cts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": symbol,
            "b": message.get("b", []),
            "a": message.get("a", []),
            "u": update_id,
            "seq": update_id,
        },
    }


def normalize_bitget_raw(message: dict, item: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将Bitget原始消息归一化为Bybit样式。"""
    inst_id = message.get("arg", {}).get("instId", symbol)
    seq = int(item.get("seq", "0") or 0)
    ts_ms = int(item.get("ts", "0") or 0)
    action = message.get("action", "")
    return {
        "topic": f"orderbook.{depth}.{inst_id}",
        "symbol": inst_id,
        "type": "snapshot" if action == "snapshot" else "delta",
        "ts": ts_ms,
        "cts": ts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": inst_id,
            "b": item.get("bids", []),
            "a": item.get("asks", []),
            "u": seq,
            "seq": seq,
        },
    }


def normalize_okx_levels(levels: list) -> list:
    """裁剪OKX盘口字段为价格和数量。"""
    return [[level[0], level[1]] for level in levels if len(level) >= 2]


def normalize_okx_raw(message: dict, item: dict, collect_ts: int, symbol: str, depth: int) -> dict:
    """将OKX原始消息归一化为Bybit样式。"""
    inst_id = message.get("arg", {}).get("instId", symbol)
    asks = normalize_okx_levels(item.get("asks", []))
    bids = normalize_okx_levels(item.get("bids", []))
    update_id = int(item.get("seqId", item.get("ts", "0")) or 0)
    previous_id = int(item.get("prevSeqId", update_id) or update_id)
    ts_ms = int(item.get("ts", "0") or 0)
    action = message.get("action", "")
    return {
        "topic": f"orderbook.{depth}.{inst_id}",
        "symbol": inst_id,
        "type": "snapshot" if action == "snapshot" else "delta",
        "ts": ts_ms,
        "cts": ts_ms,
        "collect_ts": collect_ts,
        "data": {
            "s": inst_id,
            "b": bids,
            "a": asks,
            "u": update_id,
            "seq": previous_id,
        },
    }


def apply_binance_message(orderbook: dict, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[dict, dict]:
    """处理Binance消息并生成快照。"""
    raw_record = normalize_binance_raw(message, collect_ts, message.get("s", symbol), depth)
    replace_orderbook(orderbook, message.get("b", []), message.get("a", []))
    trim_orderbook(orderbook)
    snapshot = build_snapshot(
        message.get("s", symbol),
        orderbook,
        "snapshot",
        int(message.get("E", "0") or 0),
        int(message.get("T", message.get("E", "0")) or 0),
        collect_ts,
        int(message.get("u", "0") or 0),
        int(message.get("u", "0") or 0),
        depth,
    )
    return raw_record, snapshot


def apply_bitget_message(orderbook: dict, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[list, list]:
    """处理Bitget消息并生成快照列表。"""
    raw_records = []
    if message.get("event") == "subscribe":
        return raw_records, []
    action = message.get("action", "")
    snapshots = []
    for item in message.get("data", []):
        raw_records.append(normalize_bitget_raw(message, item, collect_ts, symbol, depth))
        if action == "snapshot":
            replace_orderbook(orderbook, item.get("bids", []), item.get("asks", []))
        elif action == "update":
            apply_orderbook_delta(orderbook, item.get("bids", []), item.get("asks", []))
        else:
            continue
        trim_orderbook(orderbook)
        ts_ms = int(item.get("ts", "0") or 0)
        seq = int(item.get("seq", "0") or 0)
        snapshots.append(
            build_snapshot(
                message.get("arg", {}).get("instId", symbol),
                orderbook,
                "snapshot" if action == "snapshot" else "delta",
                ts_ms,
                ts_ms,
                collect_ts,
                seq if seq else None,
                seq if seq else None,
                depth,
            )
        )
    return raw_records, snapshots


def apply_okx_message(orderbook: dict, symbol: str, message: dict, collect_ts: int, depth: int) -> tuple[list, list]:
    """处理OKX消息并生成快照列表。"""
    raw_records = []
    if message.get("event") == "subscribe":
        return raw_records, []
    action = message.get("action", "")
    snapshots = []
    for item in message.get("data", []):
        raw_records.append(normalize_okx_raw(message, item, collect_ts, symbol, depth))
        asks = normalize_okx_levels(item.get("asks", []))
        bids = normalize_okx_levels(item.get("bids", []))
        if action == "snapshot":
            replace_orderbook(orderbook, bids, asks)
        elif action == "update":
            apply_orderbook_delta(orderbook, bids, asks)
        else:
            continue
        trim_orderbook(orderbook)
        ts_ms = int(item.get("ts", "0") or 0)
        update_id = int(item.get("seqId", item.get("ts", "0")) or 0)
        previous_id = int(item.get("prevSeqId", update_id) or update_id)
        snapshots.append(
            build_snapshot(
                message.get("arg", {}).get("instId", symbol),
                orderbook,
                "snapshot" if action == "snapshot" else "delta",
                ts_ms,
                ts_ms,
                collect_ts,
                update_id if update_id else None,
                previous_id if previous_id else None,
                depth,
            )
        )
    return raw_records, snapshots


def write_json_line(writer, payload: dict):
    """写入单行JSON记录。"""
    line = json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n"
    writer["buffer"].append(line)
    writer["buffer_bytes"] += len(line)
    now_ts = time.monotonic()
    if (
        len(writer["buffer"]) >= WS_WRITE_BUFFER_LINES
        or writer["buffer_bytes"] >= WS_WRITE_BUFFER_BYTES
        or now_ts - writer["last_flush"] >= WS_WRITE_BUFFER_INTERVAL_SECONDS
    ):
        flush_writer_buffer(writer)
    return writer


def build_snapshot_file_path(base_dir: Path, symbol: str, tag: str, snapshot: dict) -> Path:
    """按快照时间构造输出文件路径。"""
    return build_file_path(base_dir, symbol, hour_str_from_ms(snapshot["collect_ts"]), tag)


def flush_second_snapshot(last_snapshot: dict | None, writer, base_dir: Path, tag: str, symbol: str):
    """将上一秒快照写入秒级文件。"""
    if not last_snapshot:
        return writer
    snapshot_hour = hour_str_from_ms(last_snapshot["collect_ts"])
    writer = ensure_writer(base_dir, symbol, snapshot_hour, tag, writer)
    writer = write_json_line(writer, last_snapshot)
    return writer


def is_active_role(state: dict, role: str) -> bool:
    """判断指定角色是否为当前主用连接。"""
    with state["lock"]:
        return state["active_role"] == role


def run_session(exchange: str, market: str, symbol: str, role: str, ws_url: str, stop_event: threading.Event, state: dict) -> None:
    """运行单个角色的一次WS会话。"""
    depth = ORDERBOOK_DEPTH_FUTURE if market == "future" else ORDERBOOK_DEPTH_SPOT
    rt_dir, rt_ss_dir, rt_ss_1s_dir, rt_tag, rt_ss_tag, rt_ss_1s_tag = build_dirs(exchange, market)
    try:
        ws = connect_ws(ws_url)
        send_subscribe(ws, exchange, market, symbol, depth)
        update_shared_status(state, exchange, market, symbol, role, connected=True, status_text="已连接 0", recv_count=0)
    except websocket.WebSocketException as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接异常")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}连接异常，准备重连: {exc}", market)
        return
    except TimeoutError as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接超时")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}连接超时，准备重连: {exc}", market)
        return
    except OSError as exc:
        update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="网络错误")
        switch_active_role(state, exchange, market, symbol, role)
        log(f"{exchange} {market} {symbol} {role_label(role)}网络错误，准备重连: {exc}", market)
        return

    orderbook = {"bids": {}, "asks": {}}
    rt_writer = None
    rt_ss_writer = None
    rt_ss_1s_writer = None
    recv_count = 0
    last_status_ts = time.monotonic()
    last_second = None
    last_snapshot = None
    heartbeat_closed = threading.Event()

    def keepalive() -> None:
        """后台发送心跳包。"""
        payload = ping_payload(exchange)
        if payload is None:
            return
        while not stop_event.is_set() and not heartbeat_closed.is_set() and not EXIT_REQUESTED.is_set():
            time.sleep(WS_PING_INTERVAL_SECONDS)
            if stop_event.is_set() or heartbeat_closed.is_set() or EXIT_REQUESTED.is_set():
                return
            try:
                ws.send(payload)
            except websocket.WebSocketException:
                return
            except TimeoutError:
                return
            except OSError:
                return

    keepalive_thread = threading.Thread(target=keepalive, daemon=True)
    keepalive_thread.start()

    while not stop_event.is_set() and not EXIT_REQUESTED.is_set():
        try:
            raw = ws.recv()
        except websocket.WebSocketException as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接异常")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}连接异常，准备重连: {exc}", market)
            break
        except TimeoutError as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接超时")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}连接超时，准备重连: {exc}", market)
            break
        except OSError as exc:
            update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="网络错误")
            switch_active_role(state, exchange, market, symbol, role)
            log(f"{exchange} {market} {symbol} {role_label(role)}网络错误，准备重连: {exc}", market)
            break
        if raw == "pong":
            continue
        recv_count += 1
        now_status_ts = time.monotonic()
        if now_status_ts - last_status_ts >= WS_STATUS_INTERVAL_SECONDS:
            update_shared_status(state, exchange, market, symbol, role, connected=True, status_text=f"已连接 {recv_count}", recv_count=recv_count)
            last_status_ts = now_status_ts
        collect_ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        message = json.loads(raw)
        snapshots = []
        if exchange == "binance":
            raw_record, snapshot = apply_binance_message(orderbook, symbol, message, collect_ts, depth)
            raw_records = [raw_record]
            snapshots.append(snapshot)
        elif exchange == "bitget":
            raw_records, snapshots = apply_bitget_message(orderbook, symbol, message, collect_ts, depth)
        else:
            raw_records, snapshots = apply_okx_message(orderbook, symbol, message, collect_ts, depth)

        if not is_active_role(state, role):
            rt_writer, rt_ss_writer, rt_ss_1s_writer = close_writers(rt_writer, rt_ss_writer, rt_ss_1s_writer)
            continue

        hour_str = hour_str_from_ms(collect_ts)
        rt_writer = ensure_writer(rt_dir, symbol, hour_str, rt_tag, rt_writer)
        for raw_record in raw_records:
            rt_writer = write_json_line(rt_writer, raw_record)
        for snapshot in snapshots:
            snapshot_hour = hour_str_from_ms(snapshot["collect_ts"])
            rt_ss_writer = ensure_writer(rt_ss_dir, symbol, snapshot_hour, rt_ss_tag, rt_ss_writer)
            rt_ss_writer = write_json_line(rt_ss_writer, snapshot)
            second_bucket = int(snapshot["collect_ts"] / 1000)
            if last_second is None:
                last_second = second_bucket
            if second_bucket != last_second and last_snapshot:
                rt_ss_1s_writer = flush_second_snapshot(last_snapshot, rt_ss_1s_writer, rt_ss_1s_dir, rt_ss_1s_tag, symbol)
                last_second = second_bucket
            last_snapshot = snapshot

    heartbeat_closed.set()
    if last_snapshot and is_active_role(state, role):
        rt_ss_1s_writer = flush_second_snapshot(last_snapshot, rt_ss_1s_writer, rt_ss_1s_dir, rt_ss_1s_tag, symbol)
    rt_writer, rt_ss_writer, rt_ss_1s_writer = close_writers(rt_writer, rt_ss_writer, rt_ss_1s_writer)
    ws.close()
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="连接关闭")
    switch_active_role(state, exchange, market, symbol, role)


def run_role_loop(exchange: str, market: str, symbol: str, role: str, ws_url: str, stop_event: threading.Event, state: dict) -> None:
    """持续维护单个角色的WS连接。"""
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="准备连接", recv_count=0)
    while not stop_event.is_set() and not EXIT_REQUESTED.is_set():
        run_session(exchange, market, symbol, role, ws_url, stop_event, state)
        if stop_event.is_set() or EXIT_REQUESTED.is_set():
            break
        time.sleep(WS_RECONNECT_INTERVAL_SECONDS)
    update_shared_status(state, exchange, market, symbol, role, connected=False, status_text="已停止")


def run_symbol_loop(exchange: str, market: str, symbol: str, stop_event: threading.Event) -> None:
    """持续维护单个交易对的主备WS连接。"""
    log(f"{exchange} {market} {symbol} 主备订阅启动", market)
    state = build_session_state()
    primary_url, backup_url = build_ws_urls(exchange, market, symbol)
    primary_thread = threading.Thread(
        target=run_role_loop,
        args=(exchange, market, symbol, PRIMARY_ROLE, primary_url, stop_event, state),
        daemon=True,
    )
    backup_thread = threading.Thread(
        target=run_role_loop,
        args=(exchange, market, symbol, BACKUP_ROLE, backup_url, stop_event, state),
        daemon=True,
    )
    primary_thread.start()
    backup_thread.start()
    while not EXIT_REQUESTED.is_set() and (primary_thread.is_alive() or backup_thread.is_alive()):
        primary_thread.join(timeout=1.0)
        backup_thread.join(timeout=1.0)
    stop_event.set()
    primary_thread.join(timeout=1.0)
    backup_thread.join(timeout=1.0)
    status_update(exchange, market, symbol, None)
    log(f"{exchange} {market} {symbol} 主备订阅停止", market)


def sleep_with_exit(seconds: int | float) -> None:
    """按秒等待并响应退出事件。"""
    deadline = time.time() + max(0.0, float(seconds))
    while not EXIT_REQUESTED.is_set():
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        time.sleep(min(1.0, remaining))


def run_exchange_supervisor(exchange: str, market: str) -> None:
    """按交易所维护全部交易对子线程。"""
    dataset_id = "D10002-4" if market == "future" else "D10006-8"
    if not is_supported(dataset_id, exchange):
        symbol_list = get_future_symbols(exchange) if market == "future" else get_spot_symbols(exchange)
        for symbol in symbol_list:
            status_update(exchange, market, symbol, "未支持")
        return
    workers: dict[str, tuple[threading.Event, threading.Thread]] = {}
    fallback_symbols = get_future_symbols(exchange) if market == "future" else get_spot_symbols(exchange)
    while not EXIT_REQUESTED.is_set():
        try:
            desired_symbols = resolve_symbols(exchange, market)
        except NetworkRequestError as exc:
            desired_symbols = fallback_symbols
            log(f"{exchange} {market} 动态合约刷新失败，继续使用静态列表: {exc}", market)
        desired_set = set(desired_symbols)
        for symbol in sorted(desired_set - set(workers)):
            stop_event = threading.Event()
            thread = threading.Thread(target=run_symbol_loop, args=(exchange, market, symbol, stop_event), daemon=True)
            workers[symbol] = (stop_event, thread)
            thread.start()
        for symbol in sorted(set(workers) - desired_set):
            stop_event, _thread = workers.pop(symbol)
            stop_event.set()
        wait_seconds = DELIVERY_REFRESH_SECONDS if market == "future" else max(DELIVERY_REFRESH_SECONDS, 60)
        sleep_with_exit(wait_seconds)
    for symbol in sorted(workers):
        stop_event, _thread = workers.pop(symbol)
        stop_event.set()


def run_market_ws(market: str) -> None:
    """运行指定市场的多交易所订单簿WS。"""
    supervisors = []
    for exchange in list_exchanges():
        thread = threading.Thread(target=run_exchange_supervisor, args=(exchange, market), daemon=True)
        supervisors.append(thread)
        thread.start()
    while not EXIT_REQUESTED.is_set() and any(thread.is_alive() for thread in supervisors):
        for thread in supervisors:
            thread.join(timeout=1.0)


def build_ws_section_lines(task_id: str, exchange: str) -> list[str]:
    """构造单个交易所的WSS状态行。"""
    bucket = STATUS_COUNTS.get(task_id, {})
    prefix = f"{exchange}/"
    exchange_bucket = {key: value for key, value in bucket.items() if isinstance(key, str) and key.startswith(prefix)}
    if not exchange_bucket:
        return [f"{exchange.upper()}: 暂无状态"]
    lines = []
    for key in sorted(exchange_bucket):
        value = exchange_bucket[key]
        display_key = key[len(prefix) :]
        if isinstance(value, tuple) and len(value) >= 2:
            online_flag = int(value[0])
            status_text = str(value[1])
            prefix_text = "●" if online_flag > 0 else "○"
            lines.append(f"{prefix_text} {display_key}: {status_text}")
        else:
            lines.append(f"- {display_key}: {value}")
    return lines


def build_runtime_observe_text() -> str:
    """构造运行时观测文本。"""
    now_ts = time.time()
    cached_text = str(RUNTIME_OBSERVE_CACHE["text"])
    if cached_text and now_ts - float(RUNTIME_OBSERVE_CACHE["ts"]) < 1.0:
        return cached_text
    rss_text = format_bytes_text(read_process_rss_bytes())
    peak_text = format_bytes_text(read_process_peak_rss_bytes())
    upload_snapshot = get_upload_pool_snapshot()
    upload_text = (
        f"上传线程 {upload_snapshot['alive_worker_count']}/{upload_snapshot['workers']}"
        f" | 活跃 {upload_snapshot['active_count']}"
        f" | 待上传 {upload_snapshot['pending_count']}"
        f" | 速度 {format_speed_text(upload_snapshot['speed_bytes_per_second'])}"
    )
    text = f"内存: {rss_text} | 峰值: {peak_text} | {upload_text}"
    RUNTIME_OBSERVE_CACHE["ts"] = now_ts
    RUNTIME_OBSERVE_CACHE["text"] = text
    return text


def build_summary_lines() -> list[str]:
    """构造控制台状态摘要。"""
    upload_snapshot = get_upload_pool_snapshot()
    active_files = "、".join(upload_snapshot["file_names"]) if upload_snapshot["file_names"] else "-"
    pending_files = "、".join(upload_snapshot["pending_file_names"]) if upload_snapshot["pending_file_names"] else "-"
    lines = []
    lines.append("")
    lines.append(f"==== DLH WSS 状态摘要 | 存储: {DATA_STORAGE_MODE} | {time.strftime('%Y-%m-%d %H:%M:%S')} ====")
    lines.append(build_runtime_observe_text())
    lines.append("期货 WSS")
    for exchange in list_exchanges():
        if not is_supported("D10002-4", exchange):
            continue
        lines.append(f"{exchange.upper()} / D10002-4")
        lines.extend(build_ws_section_lines("D10002-4", exchange))
    lines.append("现货 WSS")
    for exchange in list_exchanges():
        if not is_supported("D10006-8", exchange):
            continue
        lines.append(f"{exchange.upper()} / D10006-8")
        lines.extend(build_ws_section_lines("D10006-8", exchange))
    lines.append("上传")
    if upload_snapshot["enabled"]:
        lines.append(f"当前上传: {active_files}")
        lines.append(f"待上传预览: {pending_files}")
    else:
        lines.append("上传池: 关闭")
    return lines


def run_summary_loop(tasks: list[Task]) -> None:
    """循环输出状态摘要。"""
    write_console_line("DLH WSS 已启动，按 Ctrl+C 退出。")
    next_summary_ts = 0.0
    while not EXIT_REQUESTED.is_set():
        now_ts = time.time()
        if now_ts >= next_summary_ts:
            for line in build_summary_lines():
                write_console_line(line)
            next_summary_ts = now_ts + SUMMARY_INTERVAL_SECONDS
        if not any(task.is_running() for task in tasks):
            break
        time.sleep(1.0)


def build_tasks() -> list[Task]:
    """构造WSS任务列表。"""
    return [
        Task("D10002-4", "D10002-4 WS", "future"),
        Task("D10006-8", "D10006-8 WS", "spot"),
    ]


def main() -> None:
    """启动单文件WSS程序。"""
    apply_storage_mode_from_argv()
    install_exception_hooks()
    signal.signal(signal.SIGINT, handle_sigint)
    if has_remove_flag():
        write_console_line("DLH WSS | 清理本地数据")
        clear_local_data(DATA_ROOT)
    write_console_line("DLH WSS | 启动中")
    if is_s3_storage_mode():
        ensure_upload_workers_started()
        write_console_line("S3启动扫描 | 已完成")
    else:
        write_console_line("S3启动扫描 | 本地模式")
    tasks = build_tasks()
    for task in tasks:
        task.start()
    run_summary_loop(tasks)
    EXIT_REQUESTED.set()
    for task in tasks:
        if task.thread:
            task.thread.join(timeout=1.0)
    restore_stdio()
    sys.__stdout__.write("\n")
    sys.__stdout__.flush()


if __name__ == "__main__":
    main()
