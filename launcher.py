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
import D10001.d10001download as d10001download
import D10005.d10005download as d10005download
import D10011.d10011 as d10011
import D10012.d10012 as d10012
import D10013.d10013download as d10013download
import D10014.d10014download as d10014download
import D10015.d10015 as d10015
import D10016.d10016 as d10016
import D10017.d10017download as d10017download
import D10018.d10018download as d10018download
import D10019.d10019download as d10019download
import D10020.d10020 as d10020
import D10021.d10021 as d10021


TASK_DEFS = [
    ("D10001", "D10001 下载", d10001download, None),
    ("D10005", "D10005 下载", d10005download, None),
    ("D10011", "D10011 处理", d10011, None),
    ("D10012", "D10012 处理", d10012, None),
    ("D10013", "D10013 下载", d10013download, None),
    ("D10014", "D10014 下载", d10014download, None),
    ("D10015", "D10015 处理", d10015, None),
    ("D10016", "D10016 处理", d10016, None),
    ("D10017", "D10017 下载", d10017download, None),
    ("D10018", "D10018 下载", d10018download, None),
    ("D10019", "D10019 下载", d10019download, None),
    ("D10020", "D10020 处理", d10020, None),
    ("D10021", "D10021 处理", d10021, None),
    ("D10002-4", "D10002-4 WS", None, "D10002-4/d10002-4ws.py"),
    ("D10006-8", "D10006-8 WS", None, "D10006-8/d10006-8ws.py"),
    ("D10022-23", "D10022-23 WS", None, "D10022-23/d10022-23ws.py"),
]  # 任务定义列表，个数


TASK_LOCAL = threading.local()  # 任务线程上下文，线程

SCHEDULE_REFRESH_SECONDS = app_config.SCHEDULE_REFRESH_SECONDS  # 触发时间刷新间隔，秒
WS_STATUS_STALE_SECONDS = app_config.WS_STATUS_STALE_SECONDS  # WS状态超时，秒
MAX_LOG_LINE_CHARS = 2000  # 单行日志最大长度，字符
TASK_FREQUENCY = {
    "D10001": "每4小时",
    "D10005": "每4小时",
    "D10011": "每日",
    "D10012": "每日",
    "D10013": "每日",
    "D10014": "每4小时",
    "D10015": "每4小时",
    "D10016": "每日",
    "D10017": "每日",
    "D10018": "每日",
    "D10019": "每日",
    "D10020": "每小时",
    "D10021": "每小时",
    "D10002-4": "实时",
    "D10006-8": "实时",
    "D10022-23": "实时",
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


def parse_download_status(text: str) -> tuple[str, str, str]:
    text = (text or "").strip()
    if not text:
        return "-", "-", "-"
    parts = text.split()
    if len(parts) >= 3 and "/" in parts[0] and len(parts[1]) == 10:
        return parts[0], parts[1], parts[2]
    if len(parts) >= 3 and parts[0] in {"重试", "今日"}:
        return parts[0], parts[1], parts[2]
    if len(parts) >= 2 and parts[0] == "对齐等待":
        return "对齐", "-", parts[1]
    if text == "已对齐":
        return "对齐", "-", "-"
    return "-", "-", truncate_by_cells(text, 60)


def build_tasks():
    tasks = []
    root = Path(__file__).resolve().parent
    for task_id, name, module, rel_path in TASK_DEFS:
        script_path = root / rel_path if rel_path else None
        tasks.append(Task(task_id, name, module, script_path))
    return tasks


def filter_tasks(tasks: list, selected: list) -> list:
    if not selected:
        return tasks
    selected_set = {item.strip() for item in selected if item.strip()}
    return [task for task in tasks if task.task_id in selected_set]


class ThreadLogWriter:
    def __init__(self, thread_task_map: dict, logs: dict, pending: dict, status_times: dict, error_logger):
        self.thread_task_map = thread_task_map
        self.logs = logs
        self.pending = pending
        self.status_times = status_times
        self.error_logger = error_logger

    def write(self, text: str) -> None:
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
                self.logs[task_id].append(line)
                self.error_logger.write(task_id, line)

    def flush(self) -> None:
        return None


class ErrorLogger:
    def __init__(self, path: Path, keywords: list, exclude_keywords: list):
        self.path = path
        self.keywords = [item.lower() for item in keywords]
        self.exclude_keywords = [item.lower() for item in exclude_keywords]
        self.trace_remaining = {}
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def should_log(self, line: str) -> bool:
        text = line.lower()
        for keyword in self.exclude_keywords:
            if keyword in text:
                return False
        for keyword in self.keywords:
            if keyword.lower() in text:
                return True
        return False

    def write(self, task_id: str, line: str) -> None:
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


def start_tasks(selected: list | None = None) -> tuple[list, dict, dict, dict, dict, dict]:
    tasks = filter_tasks(build_tasks(), selected or app_config.START_TASKS)
    if not tasks:
        print("未配置启动任务")
        return [], {}, {}, {}, {}, {}
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

    for task in tasks:
        task.start(thread_task_map, True, make_hook(task.task_id), make_log_hook(task.task_id))
    return tasks, status_counts, status_times, status_meta, logs, pending


def run_tui(stdscr, tasks, status_counts, status_times, status_meta, logs, pending) -> None:
    stdscr.nodelay(True)
    stdscr.keypad(True)
    color_enabled = curses.has_colors()
    if color_enabled:
        curses.start_color()
        curses.use_default_colors()
        curses.init_pair(1, curses.COLOR_GREEN, -1)
        curses.init_pair(2, curses.COLOR_RED, -1)
    selected = 0
    focus = "tasks"
    status_selected_index = 0
    status_scroll = 0
    prev_task_id = None
    status_height_cached = 0
    schedule_cache = {}
    last_schedule_ts = 0.0
    while True:
        stdscr.erase()
        max_rows, max_cols = stdscr.getmaxyx()
        if max_rows <= 0 or max_cols <= 0:
            time.sleep(app_config.TUI_REFRESH_SECONDS)
            continue
        list_width = max(30, max_cols // 3)
        stdscr.addstr(0, 0, truncate_by_cells("启动管理（q退出）", max(0, list_width - 1)))
        now_ts = time.time()
        if now_ts - last_schedule_ts >= SCHEDULE_REFRESH_SECONDS:
            last_schedule_ts = now_ts
            schedule_cache.clear()
            for task in tasks:
                schedule_cache[task.task_id] = compute_next_trigger(task.task_id)
        if max_rows > 1:
            stdscr.addstr(
                1,
                0,
                truncate_by_cells("任务 | 状态 | 计数 | 更新时间", max(0, list_width - 1)),
            )
        row = 2
        for idx, task in enumerate(tasks):
            if row >= max_rows:
                break
            status = "运行中" if task.is_running() else "已退出"
            counts = status_counts.get(task.task_id, {})
            total_count = 0
            for value in counts.values():
                if isinstance(value, (int, float)):
                    total_count += value
                    continue
                if isinstance(value, tuple) and value and isinstance(value[0], (int, float)):
                    total_count += value[0]
            last_ts = status_times.get(task.task_id)
            last_text = time.strftime("%H:%M:%S", time.localtime(last_ts)) if last_ts else "-"
            prefix = ">" if idx == selected else " "
            line = f"{prefix} {task.name} | {status} | {total_count} | {last_text}"
            if list_width > 1:
                stdscr.addstr(row, 0, truncate_by_cells(line, list_width - 1))
            row += 1
        list_height = max(0, row - 2)
        if tasks:
            selected = max(0, min(selected, len(tasks) - 1))
            current = tasks[selected]
            if current.task_id != prev_task_id:
                status_selected_index = 0
                status_scroll = 0
                prev_task_id = current.task_id
            log_lines = list(logs.get(current.task_id, []))
            pending_line = pending.get(current.task_id, "")
            if pending_line:
                log_lines = log_lines + [pending_line]
            log_start = 2
            log_col = list_width + 1
            if max_rows > 1 and log_col < max_cols - 1:
                if current.is_running():
                    next_text, countdown_text = schedule_cache.get(current.task_id, ("-", "-"))
                else:
                    next_text, countdown_text = "-", "-"
                schedule_header = f"倒计时: {countdown_text} | 下次触发(UTC): {next_text}"
                stdscr.addstr(0, log_col, truncate_by_cells(schedule_header, max_cols - log_col - 1))
                stdscr.addstr(1, log_col, truncate_by_cells(f"日志: {current.name}", max_cols - log_col - 1))
            status_items = []
            ws_tasks = {"D10002-4", "D10006-8", "D10022-23"}
            if current.task_id in {"D10001", "D10005", "D10013", "D10014", "D10020", "D10021", "D10002-4", "D10006-8", "D10022-23"}:
                counts = status_counts.get(current.task_id, {})
                numeric_total = 0
                for value in counts.values():
                    if isinstance(value, (int, float)):
                        numeric_total += value
                        continue
                    if isinstance(value, tuple) and value and isinstance(value[0], (int, float)):
                        numeric_total += value[0]
                if current.task_id in {"D10002-4", "D10006-8"}:
                    title = f"订阅计数（总计: {numeric_total}）"
                elif current.task_id == "D10022-23":
                    title = "市场状态"
                elif current.task_id in {"D10020", "D10021"}:
                    title = "聚合状态"
                else:
                    title = "下载状态"
                for key in sorted(counts):
                    value = counts[key]
                    if (
                        current.task_id in {"D10001", "D10005", "D10013", "D10014"}
                        and isinstance(value, tuple)
                        and len(value) >= 2
                        and isinstance(value[0], (int, float))
                    ):
                        progress, date_text, file_name = parse_download_status(str(value[1]))
                        status_items.append((key, (int(value[0]), progress, date_text, file_name), None))
                        continue
                    if isinstance(value, tuple) and len(value) >= 2 and isinstance(value[0], (int, float)):
                        line_text = f"{key}: {value[0]} {value[1]}"
                    else:
                        line_text = f"{key}: {value}"
                    if current.task_id in ws_tasks:
                        last_ts = status_meta.get(current.task_id, {}).get(key)
                        alive = bool(last_ts and (time.time() - last_ts) <= WS_STATUS_STALE_SECONDS)
                        status_items.append((key, line_text, alive))
                    else:
                        status_items.append((key, line_text, None))
            if status_items:
                available = max_cols - log_col - 1
                total = len(status_items)
                status_header = None
                if current.task_id in {"D10001", "D10005", "D10013", "D10014"}:
                    status_header = ("交易对", "完成", "进度", "日期", "文件名")
                header_rows = 1 if status_header else 0
                status_height = min(total, max(0, list_height - header_rows))
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
                stdscr.addstr(log_start, log_col, truncate_by_cells(title_line, available))
                if status_header:
                    max_symbol_cells = 0
                    max_done_cells = 0
                    max_progress_cells = 0
                    items_for_width = [item for item in status_items if isinstance(item[1], tuple)]
                    for _key, record, _alive in items_for_width:
                        if not isinstance(record, tuple) or len(record) != 4:
                            continue
                        done_count, progress, _date_text, _file_name = record
                        max_symbol_cells = max(max_symbol_cells, sum(cell_width(ch) for ch in str(_key)))
                        max_done_cells = max(max_done_cells, sum(cell_width(ch) for ch in str(done_count)))
                        max_progress_cells = max(max_progress_cells, sum(cell_width(ch) for ch in str(progress)))
                    symbol_cells = min(max(6, max_symbol_cells), 20)
                    done_cells = max(2, max_done_cells)
                    progress_cells = min(max(2, max_progress_cells), 8)
                    date_cells = 10
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
                    stdscr.addstr(log_start + 1, log_col, truncate_by_cells(header_text, available))
                for idx in range(status_height):
                    item_index = status_scroll + idx
                    if item_index >= total:
                        break
                    target_row = log_start + 1 + header_rows + idx
                    if target_row >= max_rows:
                        break
                    if available <= 0:
                        break
                    key, line_text, alive = status_items[item_index]
                    if isinstance(line_text, tuple) and len(line_text) == 4:
                        done_count, progress, date_text, file_name = line_text
                        prefix = "> " if item_index == status_selected_index else "  "
                        row_text = (
                            prefix
                            + pad_to_cells(key, symbol_cells)
                            + " "
                            + pad_to_cells(str(done_count), done_cells)
                            + " "
                            + pad_to_cells(str(progress), progress_cells)
                            + " "
                            + pad_to_cells(str(date_text), date_cells)
                            + " "
                            + truncate_by_cells(str(file_name), file_cells)
                        )
                        stdscr.addstr(target_row, log_col, truncate_by_cells(row_text, available))
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
                            stdscr.addstr(target_row, text_col, truncate_by_cells(line_text, text_available))
                    else:
                        stdscr.addstr(target_row, log_col, truncate_by_cells(line_text, available))
                log_start = log_start + status_height + 1 + header_rows
                if focus == "status":
                    selected_key = status_items[status_selected_index][0]
                    log_lines = [line for line in log_lines if selected_key in line]
            visible_lines = max_rows - log_start - 1
            tail_lines = log_lines[-visible_lines:] if visible_lines > 0 else []
            for idx, line in enumerate(tail_lines):
                text = line
                target_row = log_start + idx
                if target_row >= max_rows:
                    break
                available = max_cols - log_col - 1
                if available <= 0:
                    break
                stdscr.addstr(target_row, log_col, truncate_by_cells(text, available))
        stdscr.refresh()
        key = stdscr.getch()
        if key == ord("q"):
            break
        if key == curses.KEY_LEFT:
            focus = "tasks"
        if key == curses.KEY_RIGHT:
            focus = "status"
        if key == ord("\t"):
            focus = "status" if focus == "tasks" else "tasks"
        if focus == "tasks":
            if key == curses.KEY_UP:
                selected -= 1
            if key == curses.KEY_DOWN:
                selected += 1
        else:
            if key == curses.KEY_UP:
                status_selected_index -= 1
            if key == curses.KEY_DOWN:
                status_selected_index += 1
            if key == curses.KEY_NPAGE:
                status_selected_index += max(1, status_height_cached)
            if key == curses.KEY_PPAGE:
                status_selected_index -= max(1, status_height_cached)
        time.sleep(app_config.TUI_REFRESH_SECONDS)


def main() -> None:
    tasks, status_counts, status_times, status_meta, logs, pending = start_tasks()
    if tasks:
        curses.wrapper(run_tui, tasks, status_counts, status_times, status_meta, logs, pending)
        os._exit(0)


if __name__ == "__main__":
    main()
