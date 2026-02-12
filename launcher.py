from collections import deque
from pathlib import Path
import curses
import sys
import threading
import time
import app_config
import D10001.d10001download as d10001download
import D10005.d10005download as d10005download
import D10013.d10013download as d10013download
import D10014.d10014download as d10014download
import D10017.d10017download as d10017download
import D10018.d10018download as d10018download
import D10019.d10019download as d10019download
import d10002_4ws_module as d10002_4ws
import d10006_8ws_module as d10006_8ws
import d10022_23ws_module as d10022_23ws


TASK_DEFS = [
    ("D10001", "D10001 下载", d10001download),
    ("D10005", "D10005 下载", d10005download),
    ("D10013", "D10013 下载", d10013download),
    ("D10014", "D10014 下载", d10014download),
    ("D10017", "D10017 下载", d10017download),
    ("D10018", "D10018 下载", d10018download),
    ("D10019", "D10019 下载", d10019download),
    ("D10002-4", "D10002-4 WS", d10002_4ws),
    ("D10006-8", "D10006-8 WS", d10006_8ws),
    ("D10022-23", "D10022-23 WS", d10022_23ws),
]  # 任务定义列表，个数


class Task:
    def __init__(self, task_id: str, name: str, module):
        self.task_id = task_id
        self.name = name
        self.module = module
        self.thread = None
        self.start_time = None

    def start(self, thread_task_map: dict) -> None:
        self.start_time = time.time()
        target = self.module.run if hasattr(self.module, "run") else self.module.main
        self.thread = threading.Thread(target=target)
        self.thread.start()
        while self.thread.ident is None:
            time.sleep(0.01)
        thread_task_map[self.thread.ident] = self.task_id

    def is_running(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


def build_tasks():
    tasks = []
    for task_id, name, module in TASK_DEFS:
        tasks.append(Task(task_id, name, module))
    return tasks


def filter_tasks(tasks: list, selected: list) -> list:
    if not selected:
        return tasks
    selected_set = {item.strip() for item in selected if item.strip()}
    return [task for task in tasks if task.task_id in selected_set]


class ThreadLogWriter:
    def __init__(self, thread_task_map: dict, logs: dict, pending: dict):
        self.thread_task_map = thread_task_map
        self.logs = logs
        self.pending = pending

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

    def flush(self) -> None:
        return None


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
    writer = ThreadLogWriter(thread_task_map, logs, pending)
    sys.stdout = writer
    sys.stderr = writer

    def make_hook(task_id: str):
        def hook(key: str, count: int) -> None:
            status_counts.setdefault(task_id, {})[key] = count
            status_times[task_id] = time.time()
        return hook

    for task in tasks:
        if hasattr(task.module, "QUIET"):
            task.module.QUIET = True
        if hasattr(task.module, "STATUS_HOOK"):
            task.module.STATUS_HOOK = make_hook(task.task_id)
        task.start(thread_task_map)
    return tasks, status_counts, status_times, logs, pending


def run_tui(stdscr, tasks, status_counts, status_times, logs, pending) -> None:
    stdscr.nodelay(True)
    while True:
        stdscr.erase()
        stdscr.addstr(0, 0, "统一启动管理（按 q 退出）")
        stdscr.addstr(1, 0, "名称 | 状态 | 计数 | 最后更新时间 | 最近日志")
        row = 2
        max_cols = stdscr.getmaxyx()[1]
        for task in tasks:
            status = "运行中" if task.is_running() else "已退出"
            counts = status_counts.get(task.task_id, {})
            total_count = sum(counts.values()) if counts else 0
            last_ts = status_times.get(task.task_id)
            last_text = time.strftime("%H:%M:%S", time.localtime(last_ts)) if last_ts else "-"
            log_lines = logs.get(task.task_id, [])
            last_log = log_lines[-1] if log_lines else pending.get(task.task_id, "")
            line = f"{task.name} | {status} | {total_count} | {last_text} | {last_log}"
            if len(line) >= max_cols:
                line = line[: max_cols - 1]
            stdscr.addstr(row, 0, line)
            row += 1
        stdscr.refresh()
        key = stdscr.getch()
        if key == ord("q"):
            break
        time.sleep(app_config.TUI_REFRESH_SECONDS)


def main() -> None:
    tasks, status_counts, status_times, logs, pending = start_tasks()
    if tasks:
        curses.wrapper(run_tui, tasks, status_counts, status_times, logs, pending)


if __name__ == "__main__":
    main()
