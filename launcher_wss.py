from collections import deque
from pathlib import Path
import resource
import runpy
import signal
import sys
import threading
import time
import unicodedata

import app_config
import clear_data
from cex import cex_common
from cex import cex_config
from cex import cex_orderbook_ws_common


WS_TASK_DEFS = [
    ("D10002-4", "D10002-4 WS", Path("D10002-4/d10002-4ws.py")),  # 期货WSS脚本定义，路径
    ("D10006-8", "D10006-8 WS", Path("D10006-8/d10006-8ws.py")),  # 现货WSS脚本定义，路径
]  # WSS任务定义列表，个数
WS_LOG_LINES_PER_TASK = 8  # 单个WSS任务日志显示行数，行
UPLOAD_PENDING_PREVIEW = 8  # 上传队列预览数量，行
MAX_LOG_LINE_CHARS = 2000  # 单行日志最大长度，字符
CONSOLE_STATUS_INTERVAL_SECONDS = 60.0  # 控制台状态摘要输出间隔，秒

TASK_LOCAL = threading.local()  # 任务线程上下文，线程
EXIT_REQUESTED = threading.Event()  # 退出请求事件，事件
RUNTIME_OBSERVE_CACHE = {"ts": 0.0, "text": ""}  # 运行时观测缓存，映射
STARTUP_PROGRESS_CACHE = {"text": ""}  # 启动阶段输出缓存，映射
CONSOLE_WRITE_LOCK = threading.Lock()  # 控制台输出互斥锁，锁


def apply_storage_mode_from_argv() -> None:
    """根据启动参数设置存储模式。"""
    app_config.DATA_STORAGE_MODE = "s3" if "-s3" in sys.argv else "local"


def has_remove_flag() -> bool:
    """判断是否启用清理启动参数。"""
    return "-rm" in sys.argv


def handle_sigint(_signum, _frame) -> None:
    """统一处理Ctrl+C退出请求。"""
    EXIT_REQUESTED.set()


def restore_stdio() -> None:
    """恢复标准输出与错误输出。"""
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


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
        code = ord(ch)
        if code < 32 and ch != "\n":
            idx += 1
            continue
        out.append(ch)
        idx += 1
    cleaned = "".join(out)
    if len(cleaned) > MAX_LOG_LINE_CHARS:
        cleaned = cleaned[:MAX_LOG_LINE_CHARS]
    return cleaned


def cell_width(ch: str) -> int:
    """返回字符显示宽度。"""
    if not ch or ch == "\n":
        return 0
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in {"F", "W"} else 1


def truncate_by_cells(text: str, max_cells: int) -> str:
    """按显示宽度裁剪文本。"""
    if max_cells <= 0 or not text:
        return ""
    used = 0
    out = []
    for ch in text:
        width = cell_width(ch)
        if used + width > max_cells:
            break
        out.append(ch)
        used += width
    return "".join(out)


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


def build_runtime_observe_text(max_cells: int) -> str:
    """构造运行时观测文本。"""
    now_ts = time.time()
    cached_text = str(RUNTIME_OBSERVE_CACHE["text"])
    if cached_text and now_ts - float(RUNTIME_OBSERVE_CACHE["ts"]) < 1.0:
        return truncate_by_cells(cached_text, max_cells)
    rss_text = format_bytes_text(read_process_rss_bytes())
    peak_text = format_bytes_text(read_process_peak_rss_bytes())
    ws_snapshot = cex_orderbook_ws_common.get_all_market_buffer_snapshots()
    future_snapshot = ws_snapshot["future"]
    spot_snapshot = ws_snapshot["spot"]
    text = (
        f"内存: {rss_text} | "
        f"峰值: {peak_text} | "
        f"WS缓存 future {future_snapshot['file_count']}文件/{future_snapshot['line_count']}行 | "
        f"spot {spot_snapshot['file_count']}文件/{spot_snapshot['line_count']}行"
    )
    RUNTIME_OBSERVE_CACHE["ts"] = now_ts
    RUNTIME_OBSERVE_CACHE["text"] = text
    return truncate_by_cells(text, max_cells)


def update_startup_progress(startup_progress: dict | None, phase: str, current: int, total: int, detail: str) -> None:
    """更新启动阶段进度。"""
    if startup_progress is None:
        return
    startup_progress["phase"] = phase
    startup_progress["current"] = current
    startup_progress["total"] = total
    startup_progress["detail"] = detail
    startup_progress["updated_at"] = time.time()


def format_startup_progress_line(startup_progress: dict, max_cols: int) -> str:
    """格式化启动阶段进度文本。"""
    total = int(startup_progress.get("total") or 0)
    current = int(startup_progress.get("current") or 0)
    phase = str(startup_progress.get("phase") or "准备启动")
    detail = str(startup_progress.get("detail") or "-")
    if total > 0:
        text = f"{phase} {current}/{total} | {detail}"
    else:
        text = f"{phase} | {detail}"
    return truncate_by_cells(text, max_cols)


def render_pre_tui(startup_progress: dict) -> None:
    """在启动阶段输出一次性进度日志。"""
    max_cols = 120
    lines = []
    lines.append("DLH WSS | 启动中")
    lines.append("")
    lines.append(format_startup_progress_line(startup_progress, max_cols))
    if app_config.DATA_STORAGE_MODE == "s3":
        snapshot = cex_common.get_upload_startup_snapshot()
        sync_total = int(snapshot.get("total") or 0)
        sync_current = int(snapshot.get("current") or 0)
        sync_file = str(snapshot.get("file_name") or "-")
        sync_phase = str(snapshot.get("phase") or "未开始")
        sync_queued = int(snapshot.get("queued") or 0)
        sync_deleted = int(snapshot.get("deleted") or 0)
        if sync_total > 0:
            lines.append(f"S3启动扫描 {sync_current}/{sync_total} | {sync_phase}")
        else:
            lines.append(f"S3启动扫描 | {sync_phase}")
        lines.append(f"当前文件: {sync_file}")
        lines.append(f"已入队: {sync_queued} | 已清理: {sync_deleted}")
    else:
        lines.append("S3启动扫描 | 本地模式")
    text = "\n".join(lines)
    if STARTUP_PROGRESS_CACHE["text"] == text:
        return
    STARTUP_PROGRESS_CACHE["text"] = text
    write_console_block(lines)


def write_console_line(text: str) -> None:
    """线程安全输出单行控制台日志。"""
    with CONSOLE_WRITE_LOCK:
        sys.__stdout__.write(f"{text}\n")
        sys.__stdout__.flush()


def write_console_block(lines: list[str]) -> None:
    """线程安全输出多行控制台日志。"""
    with CONSOLE_WRITE_LOCK:
        sys.__stdout__.write("\n".join(lines) + "\n")
        sys.__stdout__.flush()


def remove_local_runtime_state(startup_progress: dict | None) -> None:
    """清理本地数据与断点状态。"""
    update_startup_progress(startup_progress, "清理本地缓存", 0, 1, "删除本地数据与失败记录")
    cex_common.reset_upload_runtime()
    clear_data.DATA_DIR.mkdir(parents=True, exist_ok=True)
    clear_data.clear_data(clear_data.DATA_DIR)
    failure_paths = [
        Path("D10001/download_failures.json"),
        Path("D10005/download_failures.json"),
        Path("D10013/download_failures.json"),
        Path("D10014/download_failures.json"),
    ]
    for path in failure_paths:
        if path.exists():
            path.unlink()
    update_startup_progress(startup_progress, "清理本地缓存", 1, 1, "已完成本地重置")


def split_status_bucket_by_exchange(bucket: dict, exchange: str) -> tuple[dict, dict]:
    """拆分交易所状态与共享状态。"""
    exchange_bucket = {}
    shared_bucket = {}
    prefix = f"{exchange}/"
    for key, value in bucket.items():
        if isinstance(key, str) and key.startswith(prefix):
            exchange_bucket[key] = value
        elif isinstance(key, str) and "/" not in key:
            shared_bucket[key] = value
    return exchange_bucket, shared_bucket


def get_status_bucket_for_exchange(task_id: str, exchange: str, status_counts: dict) -> dict:
    """返回当前交易所应显示的状态桶。"""
    if not cex_config.is_supported(task_id, exchange):
        return {}
    bucket = status_counts.get(task_id, {})
    exchange_bucket, shared_bucket = split_status_bucket_by_exchange(bucket, exchange)
    return exchange_bucket if exchange_bucket else shared_bucket


def simplify_status_key(exchange: str, key: str) -> str:
    """简化状态键显示文本。"""
    prefix = f"{exchange}/"
    if isinstance(key, str) and key.startswith(prefix):
        return key[len(prefix) :]
    return str(key)


def extract_exchange_from_log(message: str) -> str | None:
    """从日志前缀中提取交易所名称。"""
    lowered = message.lower().lstrip()
    for exchange in cex_config.list_exchanges():
        if lowered.startswith(exchange):
            return exchange
    return None


def install_thread_task_inheritance(thread_task_map: dict) -> None:
    """安装线程任务继承补丁。"""
    original_init = threading.Thread.__init__
    original_start = threading.Thread.start

    def patched_init(self, *args, **kwargs):
        target = kwargs.get("target")
        task_id = getattr(TASK_LOCAL, "task_id", None)
        if task_id and target:
            if "daemon" not in kwargs:
                kwargs["daemon"] = True

            def wrapped_target(*a, **k):
                TASK_LOCAL.task_id = task_id
                thread_task_map[threading.get_ident()] = task_id
                return target(*a, **k)

            kwargs["target"] = wrapped_target
        original_init(self, *args, **kwargs)

    def patched_start(self, *args, **kwargs):
        if getattr(TASK_LOCAL, "task_id", None):
            self.daemon = True
        return original_start(self, *args, **kwargs)

    if getattr(threading.Thread.__init__, "__name__", "") != getattr(patched_init, "__name__", ""):
        threading.Thread.__init__ = patched_init
        threading.Thread.start = patched_start


class ErrorLogger:
    def __init__(self, path: Path, keywords: list, exclude_keywords: list):
        """初始化错误日志写入器。"""
        self.path = path
        self.keywords = [item.lower() for item in keywords]
        self.exclude_keywords = [item.lower() for item in exclude_keywords]
        self.trace_remaining = {}
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def should_log(self, line: str) -> bool:
        """判断是否需要落入错误日志。"""
        text = line.lower()
        for keyword in self.exclude_keywords:
            if keyword in text:
                return False
        for keyword in self.keywords:
            if keyword in text:
                return True
        return False

    def write(self, task_id: str, line: str) -> None:
        """写入单行错误日志。"""
        remaining = self.trace_remaining.get(task_id, 0)
        if remaining > 0:
            self.trace_remaining[task_id] = remaining - 1
        elif not self.should_log(line):
            return
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        entry = f"{ts_text} [{task_id}] {line}\n"
        with self.lock:
            with self.path.open("a", encoding="utf-8") as file_obj:
                file_obj.write(entry)
        if "traceback (most recent call last):" in line.lower():
            self.trace_remaining[task_id] = 200


class ThreadLogWriter:
    def __init__(self, thread_task_map: dict, logs: dict, pending: dict, status_times: dict, error_logger: ErrorLogger):
        """初始化线程日志写入器。"""
        self.thread_task_map = thread_task_map
        self.logs = logs
        self.pending = pending
        self.status_times = status_times
        self.error_logger = error_logger
        self.trace_suppressing = {}

    def is_traceback_start(self, line: str) -> bool:
        """判断是否为异常堆栈起始行。"""
        return "traceback (most recent call last):" in line.lower()

    def is_exception_summary(self, line: str) -> bool:
        """判断是否为异常摘要行。"""
        text = line.strip()
        if not text or text.startswith("File "):
            return False
        head = text.split(":", 1)[0]
        return head.endswith("Error") or head.endswith("Exception")

    def is_traceback_chain_marker(self, line: str) -> bool:
        """判断是否为异常链提示行。"""
        text = line.strip().lower()
        if text.startswith("exception in thread "):
            return True
        return (
            "during handling of the above exception, another exception occurred:" in text
            or "the above exception was the direct cause of the following exception:" in text
        )

    def append_error_notice(self, task_id: str) -> None:
        """追加异常摘要提示。"""
        if self.logs[task_id] and self.logs[task_id][-1] == "任务异常，详情见 logs/error.log":
            return
        self.logs[task_id].append("任务异常，详情见 logs/error.log")
        write_console_line(f"[{task_id}] 任务异常，详情见 logs/error.log")

    def write(self, text: str) -> None:
        """写入一段线程日志文本。"""
        if not text:
            return
        task_id = self.thread_task_map.get(threading.get_ident()) or getattr(TASK_LOCAL, "task_id", None)
        if not task_id:
            return
        self.status_times[task_id] = time.time()
        if "\r" in text:
            self.pending[task_id] = ""
            text = text.rsplit("\r", 1)[-1].lstrip()
        text = sanitize_log_text(text)
        if not text:
            return
        buffer = self.pending.get(task_id, "") + text
        parts = buffer.split("\n")
        self.pending[task_id] = parts.pop()
        for line in parts:
            if not line:
                continue
            self.error_logger.write(task_id, line)
            if self.trace_suppressing.get(task_id):
                if self.is_traceback_chain_marker(line):
                    self.append_error_notice(task_id)
                    self.trace_suppressing[task_id] = True
                    continue
                if self.is_exception_summary(line):
                    self.trace_suppressing[task_id] = False
                continue
            if self.is_traceback_start(line) or self.is_traceback_chain_marker(line):
                self.append_error_notice(task_id)
                self.trace_suppressing[task_id] = True
                continue
            self.logs[task_id].append(line)
            write_console_line(f"[{task_id}] {line}")

    def flush(self) -> None:
        """兼容标准输出刷新接口。"""
        return None


class Task:
    def __init__(self, task_id: str, name: str, script_path: Path):
        """初始化单个任务对象。"""
        self.task_id = task_id
        self.name = name
        self.script_path = script_path
        self.thread = None
        self.start_time = None

    def start(self, thread_task_map: dict, quiet: bool, status_hook, log_hook) -> None:
        """启动任务线程。"""
        self.start_time = time.time()
        script_path = self.script_path

        def target():
            runpy.run_path(
                str(script_path),
                run_name="__main__",
                init_globals={"QUIET": quiet, "STATUS_HOOK": status_hook, "LOG_HOOK": log_hook},
            )

        def run_target():
            TASK_LOCAL.task_id = self.task_id
            thread_task_map[threading.get_ident()] = self.task_id
            target()

        self.thread = threading.Thread(target=run_target, name=self.task_id, daemon=True)
        self.thread.start()

    def is_running(self) -> bool:
        """判断任务线程是否仍在运行。"""
        return self.thread is not None and self.thread.is_alive()


def build_tasks() -> list[Task]:
    """构造WSS任务对象列表。"""
    root = Path(__file__).resolve().parent
    return [Task(task_id, name, root / rel_path) for task_id, name, rel_path in WS_TASK_DEFS]


def build_recent_log_lines(logs: dict) -> list[str]:
    """构造最近日志列表。"""
    lines = []
    for task_id, _name, _script_path in WS_TASK_DEFS:
        for line in list(logs.get(task_id, []))[-WS_LOG_LINES_PER_TASK:]:
            lines.append(f"[{task_id}] {line}")
    return lines[-(WS_LOG_LINES_PER_TASK * len(WS_TASK_DEFS)) :]


def build_ws_section_lines(task_id: str, exchange: str, status_counts: dict) -> list[str]:
    """构造单个交易所的WSS状态行。"""
    bucket = get_status_bucket_for_exchange(task_id, exchange, status_counts)
    if not bucket:
        return [f"{exchange.upper()}: 暂无状态"]
    lines = []
    for key in sorted(bucket):
        value = bucket[key]
        display_key = simplify_status_key(exchange, key)
        if isinstance(value, tuple) and len(value) >= 2:
            online_flag = int(value[0])
            status_text = str(value[1])
            prefix = "●" if online_flag > 0 else "○"
            lines.append(f"{prefix} {display_key}: {status_text}")
            continue
        lines.append(f"- {display_key}: {value}")
    return lines


def build_upload_pool_text(max_cells: int) -> str:
    """构造上传池状态文本。"""
    snapshot = cex_common.get_upload_pool_snapshot()
    if not snapshot["enabled"]:
        return "上传池: 关闭"
    file_text = "、".join(snapshot["file_names"]) if snapshot["file_names"] else "-"
    text = (
        f"上传池: 线程 {snapshot['alive_worker_count']}/{snapshot['workers']} | "
        f"活跃 {snapshot['active_count']} | "
        f"待上传: {snapshot['pending_count']} | "
        f"速度: {format_speed_text(snapshot['speed_bytes_per_second'])} | "
        f"文件: {file_text}"
    )
    return truncate_by_cells(text, max_cells)


def build_upload_lines() -> list[str]:
    """构造上传状态行。"""
    snapshot = cex_common.get_upload_pool_snapshot()
    lines = [build_upload_pool_text(400)]
    active_files = snapshot["file_names"] or ["当前无活跃上传"]
    pending_files = snapshot["pending_file_names"][:UPLOAD_PENDING_PREVIEW] or ["当前无待上传文件"]
    lines.append("当前上传:")
    for file_name in active_files:
        lines.append(f"- {file_name}")
    lines.append("待上传预览:")
    for file_name in pending_files:
        lines.append(f"- {file_name}")
    return lines


def build_console_summary_lines(status_counts: dict) -> list[str]:
    """构造控制台状态摘要。"""
    lines = []
    lines.append("")
    lines.append(f"==== DLH WSS 状态摘要 | 存储: {app_config.DATA_STORAGE_MODE} | {time.strftime('%Y-%m-%d %H:%M:%S')} ====")
    lines.append(build_runtime_observe_text(400))
    lines.append("期货 WSS")
    for exchange in cex_config.list_exchanges():
        if not cex_config.is_supported("D10002-4", exchange):
            continue
        lines.append(f"{exchange.upper()} / D10002-4")
        lines.extend(build_ws_section_lines("D10002-4", exchange, status_counts))
    lines.append("现货 WSS")
    for exchange in cex_config.list_exchanges():
        if not cex_config.is_supported("D10006-8", exchange):
            continue
        lines.append(f"{exchange.upper()} / D10006-8")
        lines.extend(build_ws_section_lines("D10006-8", exchange, status_counts))
    lines.append("上传")
    lines.extend(build_upload_lines())
    return lines


def run_wss_console(tasks, status_counts, status_times, status_meta, logs, pending) -> None:
    """运行控制台日志模式主循环。"""
    del tasks, status_times, status_meta, logs, pending
    write_console_line("DLH WSS 已启动，按 Ctrl+C 退出。")
    next_summary_ts = 0.0
    while not EXIT_REQUESTED.is_set():
        now_ts = time.time()
        if now_ts >= next_summary_ts:
            write_console_block(build_console_summary_lines(status_counts))
            next_summary_ts = now_ts + CONSOLE_STATUS_INTERVAL_SECONDS
        time.sleep(1.0)


def start_wss_tasks(startup_progress: dict | None = None) -> tuple[list, dict, dict, dict, dict, dict]:
    """启动WSS任务并返回运行时容器。"""
    tasks = build_tasks()
    status_counts = {}
    status_times = {}
    status_meta = {}
    logs = {}
    pending = {}
    thread_task_map = {}
    for task in tasks:
        logs[task.task_id] = deque(maxlen=app_config.LOG_LINES_PER_TASK)
        pending[task.task_id] = ""
    error_logger = ErrorLogger(
        Path(app_config.ERROR_LOG_PATH), app_config.ERROR_LOG_KEYWORDS, app_config.ERROR_LOG_EXCLUDE_KEYWORDS
    )
    writer = ThreadLogWriter(thread_task_map, logs, pending, status_times, error_logger)
    sys.stdout = writer
    sys.stderr = writer
    install_thread_task_inheritance(thread_task_map)

    def make_hook(task_id: str):
        """构造状态回调。"""
        def hook(key: str, value) -> None:
            bucket = status_counts.setdefault(task_id, {})
            meta = status_meta.setdefault(task_id, {})
            if value is None:
                bucket.pop(key, None)
                meta.pop(key, None)
            else:
                bucket[key] = value
                meta[key] = time.time()
            status_times[task_id] = time.time()
        return hook

    def make_log_hook(task_id: str):
        """构造日志回调。"""
        def hook(message: str) -> None:
            logs[task_id].append(message)
            error_logger.write(task_id, message)
            status_times[task_id] = time.time()
            write_console_line(f"[{task_id}] {message}")
            if "已写入:" in message:
                bucket = status_counts.setdefault(task_id, {})
                exchange = extract_exchange_from_log(message)
                key = f"{exchange}/写入" if exchange else "写入"
                bucket[key] = int(bucket.get(key) or 0) + 1
        return hook

    if app_config.DATA_STORAGE_MODE == "s3":
        update_startup_progress(startup_progress, "初始化上传池", 0, 1, "准备检查S3启动状态")
        cex_common.ensure_upload_workers_started()

    for index, task in enumerate(tasks, start=1):
        update_startup_progress(startup_progress, "启动WSS任务", index, len(tasks), task.name)
        task.start(thread_task_map, True, make_hook(task.task_id), make_log_hook(task.task_id))
    update_startup_progress(startup_progress, "启动完成", len(tasks), len(tasks), "准备进入界面")
    return tasks, status_counts, status_times, status_meta, logs, pending


def main() -> None:
    """启动仅WSS版本启动器。"""
    apply_storage_mode_from_argv()
    signal.signal(signal.SIGINT, handle_sigint)
    EXIT_REQUESTED.clear()
    startup_progress = {
        "phase": "准备启动",
        "current": 0,
        "total": 0,
        "detail": "-",
        "updated_at": time.time(),
        "result": None,
    }

    def bootstrap() -> None:
        """在后台执行WSS启动流程。"""
        if has_remove_flag():
            remove_local_runtime_state(startup_progress)
        startup_progress["result"] = start_wss_tasks(startup_progress=startup_progress)

    bootstrap_thread = threading.Thread(target=bootstrap, name="launcher-wss-bootstrap", daemon=True)
    bootstrap_thread.start()
    while bootstrap_thread.is_alive():
        if EXIT_REQUESTED.is_set():
            restore_stdio()
            sys.__stdout__.write("\n")
            sys.__stdout__.flush()
            return
        render_pre_tui(startup_progress)
        time.sleep(app_config.TUI_REFRESH_SECONDS)
    if EXIT_REQUESTED.is_set():
        restore_stdio()
        sys.__stdout__.write("\n")
        sys.__stdout__.flush()
        return
    if startup_progress["result"] is None:
        raise RuntimeError("WSS启动失败，未能生成任务列表")
    render_pre_tui(startup_progress)
    time.sleep(app_config.TUI_REFRESH_SECONDS)
    tasks, status_counts, status_times, status_meta, logs, pending = startup_progress["result"]
    run_wss_console(tasks, status_counts, status_times, status_meta, logs, pending)
    restore_stdio()
    sys.__stdout__.write("\n")
    sys.__stdout__.flush()


if __name__ == "__main__":
    main()
