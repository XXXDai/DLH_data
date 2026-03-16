from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
import curses
import math
import os
import runpy
import sys
import threading
import time
import unicodedata
import app_config
from cex import cex_common
from cex import cex_config
import D10001.d10001download as d10001download
import D10005.d10005download as d10005download
import D10013.d10013download as d10013download
import D10014.d10014download as d10014download
import D10015.d10015 as d10015
import D10016.d10016 as d10016
import D10017.d10017download as d10017download
import D10018.d10018download as d10018download
import D10019.d10019download as d10019download


def apply_storage_mode_from_argv() -> None:
    """根据启动参数设置存储模式。"""
    app_config.DATA_STORAGE_MODE = "s3" if "-s3" in sys.argv else "local"


apply_storage_mode_from_argv()


TASK_DEFS = [
    ("D10001", "D10001 下载", d10001download, None),
    ("D10005", "D10005 下载", d10005download, None),
    ("D10011", "D10011 处理", None, None),
    ("D10012", "D10012 处理", None, None),
    ("D10013", "D10013 下载", d10013download, None),
    ("D10014", "D10014 下载", d10014download, None),
    ("D10015", "D10015 处理", d10015, None),
    ("D10016", "D10016 处理", d10016, None),
    ("D10017", "D10017 下载", d10017download, None),
    ("D10018", "D10018 下载", d10018download, None),
    ("D10019", "D10019 下载", d10019download, None),
    ("D10002-4", "D10002-4 WS", None, "D10002-4/d10002-4ws.py"),
    ("D10006-8", "D10006-8 WS", None, "D10006-8/d10006-8ws.py"),
]  # 任务定义列表，个数


TASK_LOCAL = threading.local()  # 任务线程上下文，线程
UPLOAD_VIEW_ID = "__upload__"  # 上传管理页签标识，字符串
ATTACHED_TASK_PARENTS = {
    "D10011": "D10001",  # D10011附属父任务标识，字符串
    "D10012": "D10005",  # D10012附属父任务标识，字符串
}  # 附属任务父任务映射，映射

SCHEDULE_REFRESH_SECONDS = app_config.SCHEDULE_REFRESH_SECONDS  # 触发时间刷新间隔，秒
WS_STATUS_STALE_SECONDS = app_config.WS_STATUS_STALE_SECONDS  # WS状态超时，秒
MAX_LOG_LINE_CHARS = 2000  # 单行日志最大长度，字符
TASK_FREQUENCY = {
    "D10001": "每4小时",
    "D10005": "每4小时",
    "D10011": "每4小时",
    "D10012": "每4小时",
    "D10013": "每日",
    "D10014": "每4小时",
    "D10015": "每4小时",
    "D10016": "每日",
    "D10017": "每日",
    "D10018": "每日",
    "D10019": "每日",
    "D10002-4": "实时",
    "D10006-8": "实时",
}  # 任务更新频率映射，映射


def sanitize_log_text(text: str) -> str:
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


def install_thread_task_inheritance(thread_task_map: dict) -> None:
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


class Task:
    def __init__(self, task_id: str, name: str, module, script_path: Path | None):
        self.task_id = task_id
        self.name = name
        self.module = module
        self.script_path = script_path
        self.thread = None
        self.start_time = None

    def start(self, thread_task_map: dict, quiet: bool, status_hook, log_hook) -> None:
        if self.task_id in ATTACHED_TASK_PARENTS:
            self.start_time = time.time()
            return
        self.start_time = time.time()
        if self.module:
            if hasattr(self.module, "QUIET"):
                self.module.QUIET = quiet
            if hasattr(self.module, "STATUS_HOOK"):
                self.module.STATUS_HOOK = status_hook
            if hasattr(self.module, "LOG_HOOK"):
                self.module.LOG_HOOK = log_hook
            target = self.module.run if hasattr(self.module, "run") else self.module.main
        else:
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
        return self.thread is not None and self.thread.is_alive()

    def is_attached(self) -> bool:
        """判断是否为附属任务。"""
        return self.task_id in ATTACHED_TASK_PARENTS


def seconds_until_next_utc_midnight(now: datetime) -> int:
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = math.ceil((next_midnight - now).total_seconds())
    return seconds if seconds > 0 else 1


def seconds_until_next_utc_hour(now: datetime) -> int:
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    seconds = math.ceil((next_hour - now).total_seconds())
    return seconds if seconds > 0 else 1


def seconds_until_next_utc_4h(now: datetime) -> int:
    hour_block = (now.hour // 4 + 1) * 4
    if hour_block >= 24:
        next_dt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        next_dt = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)
    seconds = math.ceil((next_dt - now).total_seconds())
    return seconds if seconds > 0 else 1


def format_countdown(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 24 * 3600)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def compute_next_trigger(task_id: str) -> tuple[str, str]:
    frequency = TASK_FREQUENCY.get(task_id, "")
    now = datetime.now(tz=timezone.utc)
    if frequency == "每日":
        next_dt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        eta = seconds_until_next_utc_midnight(now)
        return next_dt.strftime("%Y-%m-%d %H:%M:%S"), format_countdown(eta)
    if frequency == "每4小时":
        hour_block = (now.hour // 4 + 1) * 4
        if hour_block >= 24:
            next_dt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            next_dt = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)
        eta = seconds_until_next_utc_4h(now)
        return next_dt.strftime("%Y-%m-%d %H:%M:%S"), format_countdown(eta)
    if frequency == "每小时":
        next_dt = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        eta = seconds_until_next_utc_hour(now)
        return next_dt.strftime("%Y-%m-%d %H:%M:%S"), format_countdown(eta)
    if frequency == "实时":
        return "持续运行", "-"
    return "-", "-"


def cell_width(ch: str) -> int:
    if not ch or ch == "\n":
        return 0
    if unicodedata.combining(ch):
        return 0
    return 2 if unicodedata.east_asian_width(ch) in {"F", "W"} else 1


def truncate_by_cells(text: str, max_cells: int) -> str:
    if max_cells <= 0 or not text:
        return ""
    used = 0
    out = []
    for ch in text:
        w = cell_width(ch)
        if used + w > max_cells:
            break
        out.append(ch)
        used += w
    return "".join(out)


def pad_to_cells(text: str, target_cells: int) -> str:
    text = text or ""
    used = 0
    for ch in text:
        used += cell_width(ch)
    if used >= target_cells:
        return truncate_by_cells(text, target_cells)
    return text + (" " * (target_cells - used))


def build_curses_attr(color_enabled: bool, pair_id: int, extra_attr: int = 0) -> int:
    """构造颜色与样式属性。"""
    if not color_enabled:
        return extra_attr
    return curses.color_pair(pair_id) | extra_attr


def draw_clipped_text(stdscr, row: int, col: int, text: str, max_cells: int, attr: int = 0) -> None:
    """按最大宽度绘制裁剪后的文本。"""
    if max_cells <= 0:
        return
    stdscr.addstr(row, col, truncate_by_cells(text, max_cells), attr)


def draw_rule(stdscr, row: int, col: int, width: int) -> None:
    """绘制水平分隔线。"""
    if width <= 0:
        return
    stdscr.hline(row, col, curses.ACS_HLINE, width)


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


def build_upload_pool_text(max_cells: int) -> str:
    """构造上传池状态文本。"""
    snapshot = cex_common.get_upload_pool_snapshot()
    if not snapshot["enabled"]:
        return "上传池: 关闭"
    file_text = "、".join(snapshot["file_names"]) if snapshot["file_names"] else "-"
    text = (
        f"上传池: {snapshot['active_count']}/{snapshot['workers']} | "
        f"待上传: {snapshot['pending_count']} | "
        f"速度: {format_speed_text(snapshot['speed_bytes_per_second'])} | "
        f"文件: {file_text}"
    )
    return truncate_by_cells(text, max_cells)


def render_upload_management(stdscr, max_cols: int, footer_row: int, header_attr: int, warm_attr: int, title_attr: int) -> None:
    """绘制上传管理页面。"""
    snapshot = cex_common.get_upload_pool_snapshot()
    section_row = 3
    subheader_row = 4
    content_row = 5
    draw_clipped_text(stdscr, section_row, 0, "上传管理", max_cols - 1, header_attr)
    summary_text = (
        f"状态: {'已初始化' if snapshot['startup_synced'] else '启动扫描中'} | "
        f"线程: {snapshot['active_count']}/{snapshot['workers']} | "
        f"待上传: {snapshot['pending_count']} | "
        f"速度: {format_speed_text(snapshot['speed_bytes_per_second'])}"
    )
    draw_clipped_text(stdscr, subheader_row, 0, summary_text, max_cols - 1, warm_attr)
    draw_clipped_text(stdscr, content_row, 0, "当前文件", max_cols - 1, header_attr)
    file_names = snapshot["file_names"] or ["当前无活跃上传"]
    row = content_row + 1
    for file_name in file_names:
        if row >= footer_row - 3:
            break
        draw_clipped_text(stdscr, row, 0, f"- {file_name}", max_cols - 1, title_attr if file_name != "当前无活跃上传" else 0)
        row += 1
    if row < footer_row - 2:
        draw_clipped_text(stdscr, row, 0, "待上传前15个", max_cols - 1, header_attr)
        row += 1
    pending_file_names = snapshot["pending_file_names"] or ["当前无待上传文件"]
    for file_name in pending_file_names:
        if row >= footer_row - 1:
            break
        draw_clipped_text(stdscr, row, 0, f"- {file_name}", max_cols - 1, 0 if file_name == "当前无待上传文件" else title_attr)
        row += 1


def parse_download_status(text: str) -> tuple[str, str, str, str]:
    """解析历史下载状态文本。"""
    text = (text or "").strip()
    if not text:
        return "-", "-", "-", "-"
    parts = text.split()
    if len(parts) >= 2 and parts[0] in {"失败", "无文件"}:
        detail_text = "请求失败" if parts[0] == "失败" else "当日无归档"
        return parts[0], parts[1], detail_text, "-"
    if len(parts) >= 3 and parts[0] in {"月", "日"} and parts[2] == "请求中":
        detail_text = "月包请求" if parts[0] == "月" else "日包请求"
        return "处理中", parts[1], detail_text, "-"
    if len(parts) >= 3 and parts[0] in {"月", "日"} and parts[2] == "准备回补":
        return "准备回补", parts[1], "本地已有进度", parts[1]
    if len(parts) >= 3 and parts[0] in {"月", "日"} and parts[2] == "已是最新":
        return "等待新文件", parts[1], "当前已是最新", parts[1]
    if len(parts) >= 3 and "/" in parts[0] and len(parts[1]) == 10:
        if parts[2] == "请求中":
            return "处理中", parts[1], parts[0], "-"
        detail_text = parts[0]
        if len(parts) >= 3:
            detail_text = f"{parts[0]} {' '.join(parts[2:])}"
        return "完成日期", parts[1], detail_text, parts[1]
    if len(parts) >= 3 and parts[0] in {"重试", "今日"}:
        return parts[0], parts[1], " ".join(parts[2:]), "-"
    if len(parts) >= 3 and parts[0] in {"月", "日"}:
        stage_text = "完成月份" if parts[0] == "月" else "完成日期"
        return stage_text, parts[1], " ".join(parts[2:]), parts[1]
    if len(parts) >= 2 and parts[0] == "准备":
        return "准备扫描", parts[1], "首个缺口", "-"
    if len(parts) >= 2 and parts[0] == "对齐等待":
        return "等待对齐", parts[1], "-", "-"
    if text == "已对齐":
        return "已对齐", "-", "-", "-"
    return "-", "-", truncate_by_cells(text, 60), "-"


def group_tasks_by_exchange(tasks: list) -> tuple[list[str], dict]:
    """按交易所归类任务列表。"""
    grouped = {}
    for exchange in cex_config.list_exchanges():
        grouped[exchange] = list(tasks)
    grouped[UPLOAD_VIEW_ID] = []
    exchanges = [exchange for exchange in cex_config.list_exchanges() if grouped.get(exchange)] + [UPLOAD_VIEW_ID]
    return exchanges, grouped


def get_view_label(view_id: str) -> str:
    """返回页签显示名称。"""
    if view_id == UPLOAD_VIEW_ID:
        return "上传管理"
    return view_id.upper()


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
    bucket = status_counts.get(task_id, {})
    exchange_bucket, shared_bucket = split_status_bucket_by_exchange(bucket, exchange)
    return exchange_bucket if exchange_bucket else shared_bucket


def get_total_count(bucket: dict) -> int:
    """汇总状态桶中的计数。"""
    total_count = 0
    for value in bucket.values():
        if isinstance(value, (int, float)):
            total_count += value
            continue
        if isinstance(value, tuple) and value and isinstance(value[0], (int, float)):
            total_count += value[0]
    return total_count


def get_last_update_text(task_id: str, exchange: str, bucket: dict, status_times: dict, status_meta: dict) -> str:
    """返回交易所视图的最近更新时间文本。"""
    if bucket:
        meta_bucket = status_meta.get(task_id, {})
        last_ts = 0.0
        for key in bucket:
            item_ts = meta_bucket.get(key) or 0.0
            if item_ts > last_ts:
                last_ts = item_ts
        if last_ts:
            return time.strftime("%H:%M:%S", time.localtime(last_ts))
    last_ts = status_times.get(task_id)
    return time.strftime("%H:%M:%S", time.localtime(last_ts)) if last_ts else "-"


def sync_text_sort_key(text: str) -> str:
    """将同步日期文本转换为可排序字符串。"""
    text = (text or "").strip()
    if len(text) == 7:
        return f"{text}-31"
    return text


def get_synced_until_text(task_id: str, bucket: dict) -> str:
    """返回任务当前已同步到的日期文本。"""
    if task_id not in {"D10001", "D10005", "D10011", "D10012", "D10013", "D10014", "D10017", "D10018", "D10019"}:
        return "-"
    latest_text = ""
    for value in bucket.values():
        if not (isinstance(value, tuple) and len(value) >= 2 and isinstance(value[0], (int, float))):
            continue
        _stage_text, _current_text, _detail_text, sync_text = parse_download_status(str(value[1]))
        if sync_text == "-":
            continue
        if sync_text_sort_key(sync_text) >= sync_text_sort_key(latest_text):
            latest_text = sync_text
    return latest_text or "-"


def simplify_status_key(exchange: str, key: str) -> str:
    """简化状态键显示文本。"""
    prefix = f"{exchange}/"
    if isinstance(key, str) and key.startswith(prefix):
        return key[len(prefix) :]
    return str(key)


def filter_logs_for_exchange(log_lines: list, exchange: str) -> list:
    """按交易所筛选日志列表。"""
    exchange_names = cex_config.list_exchanges()
    filtered = []
    for line in log_lines:
        lower_line = line.lower()
        if exchange in lower_line:
            filtered.append(line)
            continue
        if not any(name in lower_line for name in exchange_names):
            filtered.append(line)
    return filtered


def filter_logs_for_status(log_lines: list, exchange: str, status_key: str) -> list:
    """按状态项筛选日志列表。"""
    symbol_text = str(status_key).split("/")[-1]
    strict = [line for line in log_lines if exchange in line.lower() and symbol_text in line]
    if strict:
        return strict
    fuzzy = [line for line in log_lines if symbol_text in line]
    return fuzzy if fuzzy else log_lines


def build_tasks():
    tasks = []
    root = Path(__file__).resolve().parent
    for task_id, name, module, rel_path in TASK_DEFS:
        script_path = root / rel_path if rel_path else None
        tasks.append(Task(task_id, name, module, script_path))
    return tasks


def build_task_map(tasks: list[Task]) -> dict[str, Task]:
    """构造任务标识到任务对象的映射。"""
    return {task.task_id: task for task in tasks}


def filter_tasks(tasks: list, selected: list) -> list:
    if not selected:
        return tasks
    selected_set = {item.strip() for item in selected if item.strip()}
    return [task for task in tasks if task.task_id in selected_set]


class ThreadLogWriter:
    def __init__(self, thread_task_map: dict, logs: dict, pending: dict, status_times: dict, error_logger):
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
            if line:
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

    def flush(self) -> None:
        """兼容标准输出刷新接口。"""
        return None


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
            if keyword.lower() in text:
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
            with self.path.open("a", encoding="utf-8") as f:
                f.write(entry)
        if "traceback (most recent call last):" in line.lower():
            self.trace_remaining[task_id] = 200


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
    """在正式TUI前输出启动进度。"""
    max_cols = 120
    lines = []
    lines.append("DLH Data | 启动中")
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
    text = "\033[2J\033[H" + "\n".join(lines) + "\n"
    sys.__stdout__.write(text)
    sys.__stdout__.flush()


def start_tasks(selected: list | None = None, startup_progress: dict | None = None) -> tuple[list, dict, dict, dict, dict, dict]:
    """启动全部任务并返回运行时容器。"""
    update_startup_progress(startup_progress, "加载任务定义", 0, len(TASK_DEFS), "准备构建任务列表")
    tasks = filter_tasks(build_tasks(), selected or app_config.START_TASKS)
    if not tasks:
        print("未配置启动任务")
        return [], {}, {}, {}, {}, {}
    if app_config.DATA_STORAGE_MODE == "s3":
        update_startup_progress(startup_progress, "初始化上传池", 0, len(tasks), "准备检查S3启动状态")
        cex_common.ensure_upload_workers_started()
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
        def hook(message: str) -> None:
            logs[task_id].append(message)
            error_logger.write(task_id, message)
            status_times[task_id] = time.time()
            if "已写入:" in message:
                bucket = status_counts.setdefault(task_id, {})
                bucket["写入"] = int(bucket.get("写入") or 0) + 1
        return hook

    for index, task in enumerate(tasks, start=1):
        update_startup_progress(startup_progress, "启动任务", index, len(tasks), task.name)
        if task.is_attached():
            continue
        task.start(thread_task_map, True, make_hook(task.task_id), make_log_hook(task.task_id))
    update_startup_progress(startup_progress, "启动完成", len(tasks), len(tasks), "准备进入界面")
    return tasks, status_counts, status_times, status_meta, logs, pending


def run_tui(stdscr, tasks, status_counts, status_times, status_meta, logs, pending) -> None:
    """运行按交易所分组的终端界面。"""
    stdscr.nodelay(True)
    stdscr.keypad(True)
    color_enabled = curses.has_colors()
    if color_enabled:
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_RED, -1)
        curses.init_pair(3, curses.COLOR_CYAN, -1)
        curses.init_pair(4, curses.COLOR_YELLOW, -1)
    exchanges, grouped_tasks = group_tasks_by_exchange(tasks)
    task_map = build_task_map(tasks)
    if not exchanges:
        return
    selected_exchange = 0
    task_selected = {exchange: 0 for exchange in exchanges}
    task_scroll_map = {exchange: 0 for exchange in exchanges}
    status_selected_map = {}
    status_scroll_map = {}
    focus = "tasks"
    status_height_cached = 0
    schedule_cache = {}
    last_schedule_ts = 0.0
    while True:
        stdscr.erase()
        max_rows, max_cols = stdscr.getmaxyx()
        if max_rows <= 0 or max_cols <= 0:
            time.sleep(app_config.TUI_REFRESH_SECONDS)
            continue
        title_attr = build_curses_attr(color_enabled, 3, curses.A_BOLD)
        ok_attr = build_curses_attr(color_enabled, 1, curses.A_BOLD)
        bad_attr = build_curses_attr(color_enabled, 2, curses.A_BOLD)
        warm_attr = build_curses_attr(color_enabled, 4, curses.A_BOLD)
        focus_attr = curses.A_REVERSE | curses.A_BOLD
        header_attr = curses.A_BOLD
        header_row = 0
        tabs_row = 1
        rule_row = 2
        task_section_row = 3
        task_subheader_row = 4
        task_content_row = 5
        footer_row = max_rows - 1
        if max_rows < 8 or max_cols < 60:
            draw_clipped_text(stdscr, 0, 0, "窗口过小，请放大终端后查看。按 q 退出。", max_cols - 1, warm_attr)
            stdscr.refresh()
            key = stdscr.getch()
            if key == ord("q"):
                break
            time.sleep(app_config.TUI_REFRESH_SECONDS)
            continue
        current_exchange = exchanges[selected_exchange]
        exchange_tasks = grouped_tasks.get(current_exchange, [])
        if current_exchange != UPLOAD_VIEW_ID and exchange_tasks:
            task_selected[current_exchange] = max(0, min(task_selected[current_exchange], len(exchange_tasks) - 1))
            current = exchange_tasks[task_selected[current_exchange]]
        else:
            current = None
        view_label = get_view_label(current_exchange)
        if current_exchange == UPLOAD_VIEW_ID:
            title_text = f"DLH Data | 视图: {view_label} | 存储: {app_config.DATA_STORAGE_MODE}"
        else:
            title_text = (
                f"DLH Data | 交易所: {view_label} | "
                f"焦点: {'状态' if focus == 'status' else '任务'} | "
                f"存储: {app_config.DATA_STORAGE_MODE}"
            )
        draw_clipped_text(stdscr, header_row, 0, title_text, max_cols - 1, title_attr)
        if max_rows > tabs_row:
            tab_col = 0
            for idx, exchange in enumerate(exchanges):
                label = f" {get_view_label(exchange)} "
                tab_attr = focus_attr if idx == selected_exchange else header_attr
                draw_clipped_text(stdscr, tabs_row, tab_col, label, max_cols - tab_col - 1, tab_attr)
                tab_col += sum(cell_width(ch) for ch in label) + 1
                if tab_col >= max_cols - 1:
                    break
        if max_rows > rule_row:
            draw_rule(stdscr, rule_row, 0, max_cols - 1)
        if current_exchange == UPLOAD_VIEW_ID:
            render_upload_management(stdscr, max_cols, footer_row, header_attr, warm_attr, title_attr)
            footer_text = "←→ 切页签  q 退出"
            if footer_row > 0:
                draw_rule(stdscr, footer_row - 1, 0, max_cols - 1)
            draw_clipped_text(stdscr, footer_row, 0, footer_text, max_cols - 1, warm_attr)
            stdscr.refresh()
            key = stdscr.getch()
            if key == ord("q"):
                break
            if key == curses.KEY_LEFT:
                selected_exchange = (selected_exchange - 1) % len(exchanges)
            if key == curses.KEY_RIGHT:
                selected_exchange = (selected_exchange + 1) % len(exchanges)
            time.sleep(app_config.TUI_REFRESH_SECONDS)
            continue
        now_ts = time.time()
        if now_ts - last_schedule_ts >= SCHEDULE_REFRESH_SECONDS:
            last_schedule_ts = now_ts
            schedule_cache.clear()
            for task in tasks:
                schedule_cache[task.task_id] = compute_next_trigger(task.task_id)
        body_rows = max(0, footer_row - task_content_row)
        detail_reserved_rows = 10
        task_visible_rows = max(1, min(len(exchange_tasks), max(3, body_rows - detail_reserved_rows)))
        task_rule_row = min(footer_row - 1, task_content_row + task_visible_rows)
        detail_section_row = task_rule_row + 1
        detail_subheader_row = detail_section_row + 1
        detail_content_row = detail_section_row + 2
        if max_rows > task_section_row:
            task_scroll = task_scroll_map.get(current_exchange, 0)
            if task_selected[current_exchange] < task_scroll:
                task_scroll = task_selected[current_exchange]
            if task_selected[current_exchange] >= task_scroll + task_visible_rows:
                task_scroll = task_selected[current_exchange] - task_visible_rows + 1
            max_task_scroll = max(0, len(exchange_tasks) - task_visible_rows)
            task_scroll = max(0, min(task_scroll, max_task_scroll))
            task_scroll_map[current_exchange] = task_scroll
            end_idx = min(task_scroll + task_visible_rows, len(exchange_tasks))
            draw_clipped_text(
                stdscr,
                task_section_row,
                0,
                f"任务列表  {task_scroll + 1}-{end_idx}/{len(exchange_tasks)} 项",
                max_cols - 1,
                header_attr,
            )
        else:
            task_scroll = 0
        if max_rows > task_subheader_row:
            draw_clipped_text(
                stdscr,
                task_subheader_row,
                0,
                "名称               状态     计数     已同步到   更新时间",
                max_cols - 1,
                header_attr,
            )
        row = task_content_row
        name_cells = max(8, max_cols - 40)
        for idx, task in enumerate(exchange_tasks[task_scroll : task_scroll + task_visible_rows]):
            item_index = task_scroll + idx
            if row >= task_rule_row:
                break
            if cex_config.is_supported(task.task_id, current_exchange):
                if task.is_attached():
                    parent_task = task_map.get(ATTACHED_TASK_PARENTS[task.task_id])
                    status = "运行中" if parent_task and parent_task.is_running() else "已退出"
                else:
                    status = "运行中" if task.is_running() else "已退出"
            else:
                status = cex_config.UNSUPPORTED_STATUS_TEXT
            counts = get_status_bucket_for_exchange(task.task_id, current_exchange, status_counts)
            total_count = get_total_count(counts)
            synced_until_text = get_synced_until_text(task.task_id, counts)
            last_text = get_last_update_text(task.task_id, current_exchange, counts, status_times, status_meta)
            selected_attr = focus_attr if item_index == task_selected[current_exchange] and focus == "tasks" else 0
            if status == cex_config.UNSUPPORTED_STATUS_TEXT:
                status_attr = header_attr
            else:
                if task.is_attached():
                    parent_task = task_map.get(ATTACHED_TASK_PARENTS[task.task_id])
                    status_attr = ok_attr if parent_task and parent_task.is_running() else bad_attr
                else:
                    status_attr = ok_attr if task.is_running() else bad_attr
            draw_clipped_text(stdscr, row, 0, ">" if item_index == task_selected[current_exchange] else " ", 1, selected_attr)
            draw_clipped_text(stdscr, row, 2, pad_to_cells(task.name, name_cells), name_cells, selected_attr)
            draw_clipped_text(stdscr, row, 2 + name_cells + 1, pad_to_cells(status, 6), 6, selected_attr | status_attr)
            draw_clipped_text(
                stdscr,
                row,
                2 + name_cells + 1 + 7,
                pad_to_cells(str(total_count), 7),
                7,
                selected_attr,
            )
            draw_clipped_text(
                stdscr,
                row,
                2 + name_cells + 1 + 7 + 8,
                pad_to_cells(synced_until_text, 10),
                10,
                selected_attr,
            )
            draw_clipped_text(
                stdscr,
                row,
                2 + name_cells + 1 + 7 + 8 + 11,
                pad_to_cells(last_text, 8),
                8,
                selected_attr,
            )
            row += 1
        if task_rule_row < footer_row:
            draw_rule(stdscr, task_rule_row, 0, max_cols - 1)
        detail_height = max(0, footer_row - detail_content_row)
        if current:
            log_lines = list(logs.get(current.task_id, []))
            pending_line = pending.get(current.task_id, "")
            if pending_line:
                log_lines = log_lines + [pending_line]
            log_lines = filter_logs_for_exchange(log_lines, current_exchange)
            log_start = detail_content_row
            log_col = 0
            log_width = max_cols - 1
            if max_rows > detail_section_row and log_width > 0:
                draw_clipped_text(stdscr, detail_section_row, log_col, f"{current_exchange.upper()} / {current.name}", log_width, header_attr)
                if current.is_attached():
                    parent_task = task_map.get(ATTACHED_TASK_PARENTS[current.task_id])
                    task_running = bool(parent_task and parent_task.is_running())
                else:
                    task_running = current.is_running()
                if task_running:
                    next_text, countdown_text = schedule_cache.get(current.task_id, ("-", "-"))
                else:
                    next_text, countdown_text = "-", "-"
                synced_until_text = get_synced_until_text(current.task_id, get_status_bucket_for_exchange(current.task_id, current_exchange, status_counts))
                schedule_header = f"倒计时: {countdown_text} | 下次触发(UTC): {next_text} | 已同步到: {synced_until_text}"
                draw_clipped_text(stdscr, detail_subheader_row, log_col, schedule_header, log_width, warm_attr)
            status_items = []
            ws_tasks = {"D10002-4", "D10006-8", "D10022-23"}
            if current.task_id in {"D10001", "D10005", "D10011", "D10012", "D10013", "D10014", "D10017", "D10018", "D10019", "D10002-4", "D10006-8"}:
                counts = get_status_bucket_for_exchange(current.task_id, current_exchange, status_counts)
                numeric_total = get_total_count(counts)
                if current.task_id in {"D10002-4", "D10006-8"}:
                    title = f"订阅状态（在线数: {numeric_total}）"
                elif current.task_id in {"D10017", "D10018", "D10019"}:
                    title = "同步状态"
                else:
                    title = f"处理状态（已同步天数总计: {numeric_total}）" if current.task_id in {"D10011", "D10012"} else f"下载状态（已同步天数总计: {numeric_total}）"
                for key in sorted(counts):
                    value = counts[key]
                    display_key = simplify_status_key(current_exchange, key)
                    if (
                        current.task_id in {"D10001", "D10005", "D10011", "D10012", "D10013", "D10014"}
                        and isinstance(value, tuple)
                        and len(value) >= 2
                        and isinstance(value[0], (int, float))
                    ):
                        stage_text, current_text, detail_text, _sync_text = parse_download_status(str(value[1]))
                        status_items.append((key, display_key, (int(value[0]), stage_text, current_text, detail_text), None))
                        continue
                    if isinstance(value, tuple) and len(value) >= 2 and isinstance(value[0], (int, float)):
                        line_text = f"{display_key}: {value[0]} {value[1]}"
                    else:
                        line_text = f"{display_key}: {value}"
                    if current.task_id in ws_tasks:
                        last_ts = status_meta.get(current.task_id, {}).get(key)
                        alive = bool(last_ts and (time.time() - last_ts) <= WS_STATUS_STALE_SECONDS)
                        status_items.append((key, display_key, line_text, alive))
                    else:
                        status_items.append((key, display_key, line_text, None))
            if status_items:
                view_key = (current_exchange, current.task_id)
                status_selected_index = status_selected_map.get(view_key, 0)
                status_scroll = status_scroll_map.get(view_key, 0)
                available = log_width
                total = len(status_items)
                status_header = None
                if current.task_id in {"D10001", "D10005", "D10011", "D10012", "D10013", "D10014"}:
                    status_header = ("对象", "已同步天数", "阶段", "当前", "说明")
                header_rows = 1 if status_header else 0
                max_status_rows = max(0, min(total, max(4, detail_height // 2)))
                status_height = min(total, max(0, max_status_rows - header_rows))
                status_height_cached = status_height
                status_selected_index = max(0, min(status_selected_index, total - 1))
                if status_selected_index < status_scroll:
                    status_scroll = status_selected_index
                if status_selected_index >= status_scroll + status_height:
                    status_scroll = status_selected_index - status_height + 1
                max_scroll = max(0, total - status_height)
                status_scroll = max(0, min(status_scroll, max_scroll))
                if status_height > 0:
                    end_idx = min(status_scroll + status_height, total)
                    title_line = f"{title} {status_scroll + 1}-{end_idx}/{total}"
                else:
                    title_line = title
                draw_clipped_text(stdscr, log_start, log_col, title_line, available, header_attr)
                if status_header:
                    max_symbol_cells = 0
                    max_done_cells = 0
                    max_progress_cells = 0
                    items_for_width = [item for item in status_items if isinstance(item[2], tuple)]
                    for _raw_key, display_key, record, _alive in items_for_width:
                        if not isinstance(record, tuple) or len(record) != 4:
                            continue
                        done_count, stage_text, _current_text, _detail_text = record
                        max_symbol_cells = max(max_symbol_cells, sum(cell_width(ch) for ch in str(display_key)))
                        max_done_cells = max(max_done_cells, sum(cell_width(ch) for ch in str(done_count)))
                        max_progress_cells = max(max_progress_cells, sum(cell_width(ch) for ch in str(stage_text)))
                    symbol_cells = min(max(6, max_symbol_cells), 20)
                    done_cells = max(2, max_done_cells)
                    progress_cells = min(max(4, max_progress_cells), 10)
                    date_cells = 17
                    reserved_cells = symbol_cells + 1 + done_cells + 1 + progress_cells + 1 + date_cells + 1
                    file_cells = max(0, available - 2 - reserved_cells)
                    header_text = (
                        "  "
                        + pad_to_cells(status_header[0], symbol_cells)
                        + " "
                        + pad_to_cells(status_header[1], done_cells)
                        + " "
                        + pad_to_cells(status_header[2], progress_cells)
                        + " "
                        + pad_to_cells(status_header[3], date_cells)
                        + " "
                        + pad_to_cells(status_header[4], file_cells)
                    )
                    draw_clipped_text(stdscr, log_start + 1, log_col, header_text, available, header_attr)
                for idx in range(status_height):
                    item_index = status_scroll + idx
                    if item_index >= total:
                        break
                    target_row = log_start + 1 + header_rows + idx
                    if target_row >= footer_row:
                        break
                    if available <= 0:
                        break
                    key, display_key, line_text, alive = status_items[item_index]
                    if isinstance(line_text, tuple) and len(line_text) == 4:
                        done_count, stage_text, current_text, detail_text = line_text
                        prefix = "> " if item_index == status_selected_index else "  "
                        row_text = (
                            prefix
                            + pad_to_cells(display_key, symbol_cells)
                            + " "
                            + pad_to_cells(str(done_count), done_cells)
                            + " "
                            + pad_to_cells(str(stage_text), progress_cells)
                            + " "
                            + pad_to_cells(str(current_text), date_cells)
                            + " "
                            + truncate_by_cells(str(detail_text), file_cells)
                        )
                        draw_clipped_text(
                            stdscr,
                            target_row,
                            log_col,
                            row_text,
                            available,
                            focus_attr if item_index == status_selected_index and focus == "status" else 0,
                        )
                        continue
                    prefix = "> " if item_index == status_selected_index else "  "
                    line_text = prefix + line_text
                    if alive is not None:
                        dot = "●"
                        dot_width = cell_width(dot)
                        text_col = log_col
                        if available >= dot_width:
                            if color_enabled:
                                color_pair = curses.color_pair(1 if alive else 2)
                                stdscr.addstr(target_row, text_col, dot, color_pair)
                            else:
                                stdscr.addstr(target_row, text_col, dot)
                            if available >= dot_width + 1:
                                stdscr.addstr(target_row, text_col + dot_width, " ")
                                text_col = text_col + dot_width + 1
                        text_available = max_cols - text_col - 1
                        if text_available > 0:
                            draw_clipped_text(
                                stdscr,
                                target_row,
                                text_col,
                                line_text,
                                text_available,
                                focus_attr if item_index == status_selected_index and focus == "status" else 0,
                            )
                    else:
                        draw_clipped_text(
                            stdscr,
                            target_row,
                            log_col,
                            line_text,
                            available,
                            focus_attr if item_index == status_selected_index and focus == "status" else 0,
                        )
                log_start = log_start + status_height + 1 + header_rows
                status_selected_map[view_key] = status_selected_index
                status_scroll_map[view_key] = status_scroll
                if focus == "status":
                    selected_key = status_items[status_selected_index][0]
                    log_lines = filter_logs_for_status(log_lines, current_exchange, selected_key)
            if log_start < footer_row and log_width > 0:
                draw_rule(stdscr, log_start, log_col, log_width)
                log_start += 1
                draw_clipped_text(stdscr, log_start, log_col, "日志窗口", log_width, header_attr)
                log_start += 1
            visible_lines = footer_row - log_start
            tail_lines = log_lines[-visible_lines:] if visible_lines > 0 else []
            if visible_lines > 0 and not tail_lines:
                draw_clipped_text(stdscr, log_start, log_col, "当前交易所暂无匹配日志", log_width, warm_attr)
            for idx, line in enumerate(tail_lines):
                text = line
                target_row = log_start + idx
                if target_row >= footer_row:
                    break
                available = log_width
                if available <= 0:
                    break
                draw_clipped_text(stdscr, target_row, log_col, text, available)
        footer_text = "←→ 切交易所  ↑↓ 切选中项  Tab 切换焦点  PgUp/PgDn 翻状态  q 退出"
        if footer_row > 0:
            draw_rule(stdscr, footer_row - 1, 0, max_cols - 1)
        draw_clipped_text(stdscr, footer_row, 0, footer_text, max_cols - 1, warm_attr)
        stdscr.refresh()
        key = stdscr.getch()
        if key == ord("q"):
            break
        if key == curses.KEY_LEFT:
            selected_exchange = (selected_exchange - 1) % len(exchanges)
        if key == curses.KEY_RIGHT:
            selected_exchange = (selected_exchange + 1) % len(exchanges)
        if key == ord("\t"):
            focus = "status" if focus == "tasks" else "tasks"
        if focus == "tasks":
            if key == curses.KEY_UP:
                task_selected[current_exchange] -= 1
            if key == curses.KEY_DOWN:
                task_selected[current_exchange] += 1
            if exchange_tasks:
                task_selected[current_exchange] = max(0, min(task_selected[current_exchange], len(exchange_tasks) - 1))
        else:
            current_view_key = (current_exchange, current.task_id) if current else None
            status_selected_index = status_selected_map.get(current_view_key, 0) if current_view_key else 0
            if key == curses.KEY_UP:
                status_selected_index -= 1
            if key == curses.KEY_DOWN:
                status_selected_index += 1
            if key == curses.KEY_NPAGE:
                status_selected_index += max(1, status_height_cached)
            if key == curses.KEY_PPAGE:
                status_selected_index -= max(1, status_height_cached)
            if current_view_key:
                status_selected_map[current_view_key] = max(0, status_selected_index)
        time.sleep(app_config.TUI_REFRESH_SECONDS)


def main() -> None:
    """启动任务并进入TUI。"""
    startup_progress = {
        "phase": "准备启动",
        "current": 0,
        "total": 0,
        "detail": "-",
        "updated_at": time.time(),
        "result": None,
    }

    def bootstrap() -> None:
        """在后台执行启动流程。"""
        startup_progress["result"] = start_tasks(startup_progress=startup_progress)

    bootstrap_thread = threading.Thread(target=bootstrap, name="launcher-bootstrap", daemon=True)
    bootstrap_thread.start()
    while bootstrap_thread.is_alive():
        render_pre_tui(startup_progress)
        time.sleep(app_config.TUI_REFRESH_SECONDS)
    if startup_progress["result"] is None:
        raise RuntimeError("启动失败，未能生成任务列表")
    render_pre_tui(startup_progress)
    time.sleep(app_config.TUI_REFRESH_SECONDS)
    tasks, status_counts, status_times, status_meta, logs, pending = startup_progress["result"]
    if tasks:
        curses.wrapper(run_tui, tasks, status_counts, status_times, status_meta, logs, pending)
        os._exit(0)


if __name__ == "__main__":
    main()
