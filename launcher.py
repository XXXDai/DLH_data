from collections import deque
from pathlib import Path
import curses
import runpy
import sys
import threading
import time
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

        self.thread = threading.Thread(target=target)
        self.thread.start()
        while self.thread.ident is None:
            time.sleep(0.01)
        thread_task_map[self.thread.ident] = self.task_id

    def is_running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


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
    def __init__(self, thread_task_map: dict, logs: dict, pending: dict, error_logger):
        self.thread_task_map = thread_task_map
        self.logs = logs
        self.pending = pending
        self.error_logger = error_logger

    def write(self, text: str) -> None:
        if not text:
            return
        task_id = self.thread_task_map.get(threading.get_ident())
        if not task_id:
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
    def __init__(self, path: Path, keywords: list):
        self.path = path
        self.keywords = [item.lower() for item in keywords]
        self.lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def should_log(self, line: str) -> bool:
        text = line.lower()
        for keyword in self.keywords:
            if keyword.lower() in text:
                return True
        return False

    def write(self, task_id: str, line: str) -> None:
        if not self.should_log(line):
            return
        ts_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        entry = f"{ts_text} [{task_id}] {line}\n"
        with self.lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(entry)


def start_tasks(selected: list | None = None) -> tuple[list, dict, dict, dict, dict]:
    tasks = filter_tasks(build_tasks(), selected or app_config.START_TASKS)
    if not tasks:
        print("未配置启动任务")
        return [], {}, {}, {}, {}
    status_counts = {}
    status_times = {}
    logs = {}
    pending = {}
    thread_task_map = {}
    for task in tasks:
        logs[task.task_id] = deque(maxlen=app_config.LOG_LINES_PER_TASK)
        pending[task.task_id] = ""
    error_logger = ErrorLogger(Path(app_config.ERROR_LOG_PATH), app_config.ERROR_LOG_KEYWORDS)
    writer = ThreadLogWriter(thread_task_map, logs, pending, error_logger)
    sys.stdout = writer
    sys.stderr = writer

    def make_hook(task_id: str):
        def hook(key: str, count: int) -> None:
            status_counts.setdefault(task_id, {})[key] = count
            status_times[task_id] = time.time()
        return hook

    def make_log_hook(task_id: str):
        def hook(message: str) -> None:
            logs[task_id].append(message)
            error_logger.write(task_id, message)
        return hook

    for task in tasks:
        task.start(thread_task_map, True, make_hook(task.task_id), make_log_hook(task.task_id))
    return tasks, status_counts, status_times, logs, pending


def run_tui(stdscr, tasks, status_counts, status_times, logs, pending) -> None:
    stdscr.nodelay(True)
    selected = 0
    while True:
        stdscr.erase()
        max_rows, max_cols = stdscr.getmaxyx()
        stdscr.addstr(0, 0, "统一启动管理（上下键切换，q退出）")
        list_width = max_cols // 2
        stdscr.addstr(1, 0, "任务 | 状态 | 计数 | 更新时间")
        row = 2
        for idx, task in enumerate(tasks):
            status = "运行中" if task.is_running() else "已退出"
            counts = status_counts.get(task.task_id, {})
            total_count = sum(counts.values()) if counts else 0
            last_ts = status_times.get(task.task_id)
            last_text = time.strftime("%H:%M:%S", time.localtime(last_ts)) if last_ts else "-"
            prefix = ">" if idx == selected else " "
            line = f"{prefix} {task.name} | {status} | {total_count} | {last_text}"
            if len(line) >= list_width:
                line = line[: list_width - 1]
            stdscr.addstr(row, 0, line)
            row += 1
        if tasks:
            selected = max(0, min(selected, len(tasks) - 1))
            current = tasks[selected]
            log_lines = list(logs.get(current.task_id, []))
            pending_line = pending.get(current.task_id, "")
            if pending_line:
                log_lines = log_lines + [pending_line]
            log_start = 2
            log_col = list_width + 1
            stdscr.addstr(1, log_col, f"日志: {current.name}")
            visible_lines = max_rows - log_start - 1
            tail_lines = log_lines[-visible_lines:] if visible_lines > 0 else []
            for idx, line in enumerate(tail_lines):
                text = line
                if len(text) >= max_cols - log_col:
                    text = text[: max_cols - log_col - 1]
                stdscr.addstr(log_start + idx, log_col, text)
        stdscr.refresh()
        key = stdscr.getch()
        if key == ord("q"):
            break
        if key == curses.KEY_UP:
            selected -= 1
        if key == curses.KEY_DOWN:
            selected += 1
        time.sleep(app_config.TUI_REFRESH_SECONDS)


def main() -> None:
    tasks, status_counts, status_times, logs, pending = start_tasks()
    if tasks:
        curses.wrapper(run_tui, tasks, status_counts, status_times, logs, pending)


if __name__ == "__main__":
    main()
