from functools import lru_cache
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
import csv
import gzip
import json
import queue
import socket
import subprocess
import threading
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectionClosedError
from botocore.exceptions import ConnectTimeoutError
from botocore.exceptions import EndpointConnectionError
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import PartialCredentialsError
from botocore.exceptions import ReadTimeoutError
from boto3.s3.transfer import TransferConfig

import app_config
from cex import cex_config


PART_FILE_STALE_SECONDS = 30 * 60  # 临时文件过期时间，秒
FAILURE_FILE_LOCK = threading.Lock()  # 失败记录文件写入锁，锁对象
UPLOAD_QUEUE = queue.Queue()  # S3上传任务队列，队列
UPLOAD_QUEUE_LOCK = threading.Lock()  # S3上传任务去重锁，锁对象
UPLOAD_PENDING_PATHS = set()  # S3待上传文件集合，个数
UPLOAD_WORKERS_STARTED = False  # S3上传线程是否已启动，开关
UPLOAD_STARTUP_SYNC_DONE = False  # S3启动补传是否已完成，开关
UPLOAD_STATUS_LOCK = threading.Lock()  # S3上传状态锁，锁对象
UPLOAD_ACTIVE_TASKS = {}  # S3活跃上传任务映射，个数
UPLOAD_STARTUP_STATUS_LOCK = threading.Lock()  # S3启动扫描状态锁，锁对象
UPLOAD_STARTUP_STATUS = {  # S3启动扫描状态，映射
    "phase": "未开始",  # 当前阶段，字符串
    "current": 0,  # 当前进度，个数
    "total": 0,  # 总进度，个数
    "file_name": "-",  # 当前文件名，字符串
    "queued": 0,  # 已入队文件数，个数
    "deleted": 0,  # 已删除本地文件数，个数
    "done": False,  # 是否完成，开关
}  # S3启动扫描状态，映射


def is_s3_storage_mode() -> bool:
    """判断是否启用S3存储模式。"""
    return app_config.DATA_STORAGE_MODE == "s3"


def seconds_until_next_utc_midnight() -> int:
    """返回距离下一个UTC零点的秒数。"""
    now = datetime.now(tz=timezone.utc)
    next_midnight = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    seconds = int((next_midnight - now).total_seconds())
    return seconds if seconds > 0 else 1


def seconds_until_next_utc_hour() -> int:
    """返回距离下一个UTC整点的秒数。"""
    now = datetime.now(tz=timezone.utc)
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    seconds = int((next_hour - now).total_seconds())
    return seconds if seconds > 0 else 1


def seconds_until_next_utc_4h() -> int:
    """返回距离下一个UTC四小时整点的秒数。"""
    now = datetime.now(tz=timezone.utc)
    hour_block = (now.hour // 4 + 1) * 4
    if hour_block >= 24:
        next_dt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    else:
        next_dt = now.replace(hour=hour_block, minute=0, second=0, microsecond=0)
    seconds = int((next_dt - now).total_seconds())
    return seconds if seconds > 0 else 1


def iter_dates(start_date: str, end_date: str):
    """按日遍历日期字符串。"""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current = start
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        current += timedelta(days=1)


def iter_months(start_date: str, end_date: str):
    """按月遍历月份起始日期。"""
    current = datetime.strptime(start_date, "%Y-%m-%d").replace(day=1)
    end = datetime.strptime(end_date, "%Y-%m-%d").replace(day=1)
    while current <= end:
        yield current.strftime("%Y-%m-%d")
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def month_end(date_str: str) -> str:
    """返回月份结束日期。"""
    current = datetime.strptime(date_str, "%Y-%m-%d")
    if current.month == 12:
        next_month = current.replace(year=current.year + 1, month=1, day=1)
    else:
        next_month = current.replace(month=current.month + 1, day=1)
    return (next_month - timedelta(days=1)).strftime("%Y-%m-%d")


def request_json(url: str, timeout_seconds: int) -> dict:
    """请求JSON接口并返回字典。"""
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"接口请求失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("接口请求失败: 网络错误") from exc
    except TimeoutError as exc:
        raise RuntimeError("接口请求失败: 超时") from exc
    except socket.timeout as exc:
        raise RuntimeError("接口请求失败: 超时") from exc


def download_bytes(url: str, timeout_seconds: int) -> bytes:
    """下载二进制内容并返回字节串。"""
    try:
        with urlopen(url, timeout=timeout_seconds) as response:
            return response.read()
    except HTTPError as exc:
        raise RuntimeError(f"下载失败: HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError("下载失败: 网络错误") from exc
    except TimeoutError as exc:
        raise RuntimeError("下载失败: 超时") from exc
    except socket.timeout as exc:
        raise RuntimeError("下载失败: 超时") from exc


def ensure_parent(path: Path) -> None:
    """确保目标父目录存在。"""
    path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_s3_client():
    """构造S3客户端。"""
    return boto3.client(
        "s3",
        config=Config(
            connect_timeout=app_config.S3_CONNECT_TIMEOUT_SECONDS,
            read_timeout=app_config.S3_READ_TIMEOUT_SECONDS,
            retries={"max_attempts": app_config.S3_MAX_ATTEMPTS, "mode": "standard"},
            max_pool_connections=app_config.S3_MAX_POOL_CONNECTIONS,
        ),
    )


@lru_cache(maxsize=1)
def get_s3_transfer_config() -> TransferConfig:
    """构造S3传输配置。"""
    return TransferConfig(
        multipart_threshold=app_config.S3_MULTIPART_THRESHOLD_BYTES,
        max_concurrency=app_config.S3_UPLOAD_MAX_CONCURRENCY,
        multipart_chunksize=app_config.S3_MULTIPART_CHUNKSIZE_BYTES,
        use_threads=app_config.S3_USE_THREADS,
    )


def build_s3_key(file_path: Path) -> str | None:
    """构造本地文件对应的S3对象键。"""
    absolute_path = file_path.resolve()
    data_root = cex_config.DATA_DYLAN_ROOT.resolve()
    try:
        relative_path = absolute_path.relative_to(data_root)
    except ValueError:
        return None
    return f"{app_config.S3_PREFIX}/{relative_path.as_posix()}"


def is_retryable_s3_transfer_error(exc: BaseException) -> bool:
    """判断是否为可重试的S3传输异常。"""
    return isinstance(exc, (ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError, ConnectionClosedError))


def maybe_sleep_for_s3_retry(attempt: int) -> None:
    """在下一次S3重试前等待。"""
    if attempt < app_config.S3_TRANSFER_RETRY_TIMES:
        time.sleep(app_config.S3_TRANSFER_RETRY_INTERVAL_SECONDS)


def log_s3_transfer_retry(action: str, file_path: Path, attempt: int, exc: BaseException) -> None:
    """输出S3传输重试日志。"""
    print(f"S3{action}重试 {attempt}/{app_config.S3_TRANSFER_RETRY_TIMES}: {file_path} | {exc}")


def log_s3_transfer_give_up(action: str, file_path: Path, exc: BaseException) -> None:
    """输出S3传输放弃日志。"""
    print(f"S3{action}失败，已保留本地文件稍后重试: {file_path} | {exc}")


class UploadProgressTracker:
    """记录单个上传任务的进度。"""

    def __init__(self, file_path: Path):
        """初始化单个上传任务跟踪器。"""
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


def get_upload_pool_snapshot() -> dict:
    """返回上传池当前状态快照。"""
    with UPLOAD_STATUS_LOCK:
        active_tasks = list(UPLOAD_ACTIVE_TASKS.values())
    with UPLOAD_QUEUE_LOCK:
        pending_count = UPLOAD_QUEUE.qsize()
        pending_file_names = []
        for file_path in list(UPLOAD_QUEUE.queue)[:15]:
            pending_file_names.append(Path(file_path).name)
    speed_bytes_per_second = 0.0
    file_names = []
    for item in active_tasks:
        file_names.append(item["file_name"])
        elapsed_seconds = max(0.001, time.time() - float(item["started_at"]))
        speed_bytes_per_second += float(item["uploaded_bytes"]) / elapsed_seconds
    return {
        "enabled": is_s3_storage_mode(),
        "workers": app_config.S3_UPLOAD_POOL_WORKERS,
        "active_count": len(active_tasks),
        "pending_count": pending_count,
        "startup_synced": UPLOAD_STARTUP_SYNC_DONE,
        "speed_bytes_per_second": speed_bytes_per_second,
        "file_names": file_names,
        "pending_file_names": pending_file_names,
    }


def update_upload_startup_status(phase: str, current: int, total: int, file_name: str, queued: int, deleted: int, done: bool) -> None:
    """更新S3启动扫描状态。"""
    with UPLOAD_STARTUP_STATUS_LOCK:
        UPLOAD_STARTUP_STATUS["phase"] = phase
        UPLOAD_STARTUP_STATUS["current"] = current
        UPLOAD_STARTUP_STATUS["total"] = total
        UPLOAD_STARTUP_STATUS["file_name"] = file_name
        UPLOAD_STARTUP_STATUS["queued"] = queued
        UPLOAD_STARTUP_STATUS["deleted"] = deleted
        UPLOAD_STARTUP_STATUS["done"] = done


def get_upload_startup_snapshot() -> dict:
    """返回S3启动扫描状态快照。"""
    with UPLOAD_STARTUP_STATUS_LOCK:
        return dict(UPLOAD_STARTUP_STATUS)


def reset_upload_runtime() -> None:
    """重置上传池内存状态。"""
    global UPLOAD_WORKERS_STARTED
    global UPLOAD_STARTUP_SYNC_DONE
    with UPLOAD_QUEUE_LOCK:
        UPLOAD_PENDING_PATHS.clear()
        with UPLOAD_QUEUE.mutex:
            UPLOAD_QUEUE.queue.clear()
            UPLOAD_QUEUE.unfinished_tasks = 0
            UPLOAD_QUEUE.all_tasks_done.notify_all()
    with UPLOAD_STATUS_LOCK:
        UPLOAD_ACTIVE_TASKS.clear()
    with UPLOAD_STARTUP_STATUS_LOCK:
        UPLOAD_STARTUP_STATUS["phase"] = "未开始"
        UPLOAD_STARTUP_STATUS["current"] = 0
        UPLOAD_STARTUP_STATUS["total"] = 0
        UPLOAD_STARTUP_STATUS["file_name"] = "-"
        UPLOAD_STARTUP_STATUS["queued"] = 0
        UPLOAD_STARTUP_STATUS["deleted"] = 0
        UPLOAD_STARTUP_STATUS["done"] = False
    UPLOAD_WORKERS_STARTED = False
    UPLOAD_STARTUP_SYNC_DONE = False


def upload_file_to_s3_blocking(file_path: Path) -> None:
    """同步上传单个本地文件到S3。"""
    if not is_s3_storage_mode():
        return
    if not file_path.exists() or not file_path.is_file():
        return
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return
    for attempt in range(1, app_config.S3_TRANSFER_RETRY_TIMES + 1):
        tracker = UploadProgressTracker(file_path)
        try:
            get_s3_client().upload_file(
                str(file_path),
                app_config.S3_BUCKET_NAME,
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
            if attempt >= app_config.S3_TRANSFER_RETRY_TIMES:
                log_s3_transfer_give_up("上传", file_path, exc)
                return
            log_s3_transfer_retry("上传", file_path, attempt, exc)
            maybe_sleep_for_s3_retry(attempt)
        finally:
            tracker.finish()


def upload_worker_loop() -> None:
    """循环处理S3上传队列。"""
    while True:
        file_path = UPLOAD_QUEUE.get()
        try:
            try:
                upload_file_to_s3_blocking(file_path)
            except RuntimeError as exc:
                print(f"S3上传线程异常，已跳过当前文件: {file_path} | {exc}")
        finally:
            with UPLOAD_QUEUE_LOCK:
                UPLOAD_PENDING_PATHS.discard(str(file_path))
            UPLOAD_QUEUE.task_done()


def ensure_upload_workers_started() -> None:
    """确保S3上传线程池已启动。"""
    global UPLOAD_WORKERS_STARTED
    if UPLOAD_WORKERS_STARTED:
        sync_local_files_to_s3_on_startup()
        return
    with UPLOAD_QUEUE_LOCK:
        if UPLOAD_WORKERS_STARTED:
            sync_local_files_to_s3_on_startup()
            return
        for index in range(app_config.S3_UPLOAD_POOL_WORKERS):
            worker = threading.Thread(
                target=upload_worker_loop,
                name=f"s3-upload-{index + 1}",
                daemon=True,
            )
            worker.start()
        UPLOAD_WORKERS_STARTED = True
    sync_local_files_to_s3_on_startup()


def enqueue_file_for_s3_upload(file_path: Path) -> None:
    """将本地文件加入S3上传队列。"""
    if not is_s3_storage_mode():
        return
    if not file_path.exists() or not file_path.is_file():
        return
    ensure_upload_workers_started()
    file_key = str(file_path.resolve())
    with UPLOAD_QUEUE_LOCK:
        if file_key in UPLOAD_PENDING_PATHS:
            return
        UPLOAD_PENDING_PATHS.add(file_key)
    UPLOAD_QUEUE.put(file_path.resolve())


def build_s3_prefix(dir_path: Path) -> str | None:
    """构造本地目录对应的S3前缀。"""
    absolute_path = dir_path.resolve()
    data_root = cex_config.DATA_DYLAN_ROOT.resolve()
    try:
        relative_path = absolute_path.relative_to(data_root)
    except ValueError:
        return None
    prefix = f"{app_config.S3_PREFIX}/{relative_path.as_posix().rstrip('/')}"
    return prefix + "/"


def s3_object_exists(file_path: Path) -> bool:
    """判断文件是否已存在于S3。"""
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return False
    try:
        get_s3_client().head_object(Bucket=app_config.S3_BUCKET_NAME, Key=s3_key)
        return True
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
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise RuntimeError(f"S3检查失败: {code or '未知错误'}") from exc


def list_all_s3_keys_under_data_root() -> set[str]:
    """列出数据根目录下所有S3对象键。"""
    prefix = f"{app_config.S3_PREFIX}/"
    keys = set()
    try:
        paginator = get_s3_client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=app_config.S3_BUCKET_NAME, Prefix=prefix):
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
        update_upload_startup_status("已完成", 0, 0, "-", 0, 0, True)
        return
    if not is_s3_storage_mode():
        UPLOAD_STARTUP_SYNC_DONE = True
        update_upload_startup_status("本地模式", 0, 0, "-", 0, 0, True)
        return
    if not cex_config.DATA_DYLAN_ROOT.exists():
        UPLOAD_STARTUP_SYNC_DONE = True
        update_upload_startup_status("目录不存在", 0, 0, "-", 0, 0, True)
        return
    local_files = sorted([path for path in cex_config.DATA_DYLAN_ROOT.rglob("*") if path.is_file()])
    queued_count = 0
    deleted_count = 0
    update_upload_startup_status("扫描本地文件", 0, len(local_files), "-", queued_count, deleted_count, False)
    update_upload_startup_status("拉取S3文件列表", 0, len(local_files), "-", queued_count, deleted_count, False)
    existing_s3_keys = list_all_s3_keys_under_data_root()
    for index, file_path in enumerate(local_files, start=1):
        update_upload_startup_status("检查S3现状", index, len(local_files), file_path.name, queued_count, deleted_count, False)
        if file_path.name.endswith(".part"):
            continue
        s3_key = build_s3_key(file_path)
        if s3_key and s3_key in existing_s3_keys:
            file_path.unlink()
            deleted_count += 1
            update_upload_startup_status("删除本地已同步文件", index, len(local_files), file_path.name, queued_count, deleted_count, False)
            continue
        file_key = str(file_path.resolve())
        with UPLOAD_QUEUE_LOCK:
            if file_key in UPLOAD_PENDING_PATHS:
                continue
            UPLOAD_PENDING_PATHS.add(file_key)
        UPLOAD_QUEUE.put(file_path.resolve())
        queued_count += 1
        update_upload_startup_status("加入上传队列", index, len(local_files), file_path.name, queued_count, deleted_count, False)
    UPLOAD_STARTUP_SYNC_DONE = True
    update_upload_startup_status("已完成", len(local_files), len(local_files), "-", queued_count, deleted_count, True)


def list_s3_file_names(dir_path: Path) -> list[str]:
    """列出S3目录下的直接子文件名。"""
    prefix = build_s3_prefix(dir_path)
    if not prefix:
        return []
    try:
        paginator = get_s3_client().get_paginator("list_objects_v2")
        names = set()
        for page in paginator.paginate(Bucket=app_config.S3_BUCKET_NAME, Prefix=prefix):
            for item in page.get("Contents", []):
                key = str(item.get("Key") or "")
                if not key.startswith(prefix) or key.endswith("/"):
                    continue
                suffix = key[len(prefix) :]
                if "/" in suffix:
                    continue
                names.add(suffix)
        return sorted(names)
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


def list_storage_file_names(dir_path: Path) -> list[str]:
    """按当前存储模式列出目录下的直接子文件名。"""
    if not is_s3_storage_mode():
        if not dir_path.exists():
            return []
        return sorted([path.name for path in dir_path.iterdir() if path.is_file()])
    return list_s3_file_names(dir_path)


def storage_file_exists(file_path: Path) -> bool:
    """按当前存储模式判断文件是否存在。"""
    if file_path.exists():
        return True
    if not is_s3_storage_mode():
        return False
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return False
    try:
        get_s3_client().head_object(Bucket=app_config.S3_BUCKET_NAME, Key=s3_key)
        return True
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
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise RuntimeError(f"S3检查失败: {code or '未知错误'}") from exc


def download_file_from_storage(file_path: Path) -> bool:
    """按当前存储模式恢复单个文件到本地。"""
    if file_path.exists():
        return True
    if not is_s3_storage_mode():
        return False
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return False
    ensure_parent(file_path)
    tmp_path = build_part_path(file_path)
    if tmp_path.exists():
        tmp_path.unlink()
    for attempt in range(1, app_config.S3_TRANSFER_RETRY_TIMES + 1):
        try:
            get_s3_client().download_file(
                app_config.S3_BUCKET_NAME,
                s3_key,
                str(tmp_path),
                Config=get_s3_transfer_config(),
            )
            break
        except NoCredentialsError as exc:
            raise RuntimeError("S3下载失败: 缺少凭证") from exc
        except PartialCredentialsError as exc:
            raise RuntimeError("S3下载失败: 凭证不完整") from exc
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                return False
            raise RuntimeError(f"S3下载失败: {code or '未知错误'}") from exc
        except (ConnectTimeoutError, ReadTimeoutError, EndpointConnectionError, ConnectionClosedError) as exc:
            if tmp_path.exists():
                tmp_path.unlink()
            if attempt >= app_config.S3_TRANSFER_RETRY_TIMES:
                log_s3_transfer_give_up("下载", file_path, exc)
                return False
            log_s3_transfer_retry("下载", file_path, attempt, exc)
            maybe_sleep_for_s3_retry(attempt)
    if file_path.name.endswith(".csv.gz") and not is_valid_gzip_file(tmp_path):
        tmp_path.unlink()
        return False
    tmp_path.replace(file_path)
    return True


def count_existing_days(existing_dates: set[str], start_date: str, end_date: str) -> int:
    """统计区间内已存在的自然日数量。"""
    return sum(1 for date_text in iter_dates(start_date, end_date) if date_text in existing_dates)


def list_missing_dates(existing_dates: set[str], start_date: str, end_date: str) -> list[str]:
    """列出区间内缺失的自然日。"""
    return [date_text for date_text in iter_dates(start_date, end_date) if date_text not in existing_dates]


def get_synced_until_date(existing_dates: set[str], start_date: str, end_date: str) -> str:
    """返回从起始日连续同步到的最新日期。"""
    latest_text = ""
    for date_text in iter_dates(start_date, end_date):
        if date_text not in existing_dates:
            break
        latest_text = date_text
    return latest_text


def upload_file_to_s3(file_path: Path) -> None:
    """提交单个本地文件到S3上传队列。"""
    enqueue_file_for_s3_upload(file_path)


def replace_output_file(tmp_path: Path, output_path: Path) -> None:
    """原子替换正式文件并上传到S3。"""
    tmp_path.replace(output_path)
    upload_file_to_s3(output_path)


def build_part_path(path: Path) -> Path:
    """构造临时分片文件路径。"""
    return path.with_name(path.name + ".part")


def cleanup_stale_part_file(path: Path) -> bool:
    """清理过期或多余的临时分片文件。"""
    part_path = build_part_path(path)
    if not part_path.exists():
        return False
    if path.exists():
        part_path.unlink()
        return True
    age_seconds = time.time() - part_path.stat().st_mtime
    if age_seconds >= PART_FILE_STALE_SECONDS:
        part_path.unlink()
        return True
    return False


def is_valid_gzip_file(file_path: Path) -> bool:
    """检查GZip文件是否完整可读。"""
    return (
        subprocess.run(
            ["/usr/bin/gzip", "-t", str(file_path)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        ).returncode
        == 0
    )


def load_failures(path: Path) -> list:
    """读取失败记录列表。"""
    if not path.exists() or path.stat().st_size == 0:
        return []
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []
    data = json.loads(text)
    return data if isinstance(data, list) else []


def save_failures(path: Path, failures: list) -> None:
    """写入失败记录列表。"""
    ensure_parent(path)
    tmp_path = path.with_name(path.name + ".tmp")
    tmp_path.write_text(json.dumps(failures, ensure_ascii=False, indent=2), encoding="utf-8")
    replace_output_file(tmp_path, path)


def upsert_failure(failures: list, record: dict) -> None:
    """更新或追加失败记录。"""
    for index, item in enumerate(failures):
        if item.get("键") == record.get("键"):
            failures[index].update(record)
            return
    failures.append(record)


def remove_failure(failures: list, failure_key: str) -> None:
    """删除指定失败记录。"""
    failures[:] = [item for item in failures if item.get("键") != failure_key]


def update_failure_file(path: Path, record: dict | None, failure_key: str) -> list:
    """更新失败记录文件。"""
    with FAILURE_FILE_LOCK:
        failures = load_failures(path)
        if record is None:
            remove_failure(failures, failure_key)
        else:
            upsert_failure(failures, record)
        save_failures(path, failures)
        return failures


def write_gzip_csv_rows(file_path: Path, fieldnames: list[str], rows: list[dict], append: bool) -> int:
    """写入Gzip压缩CSV行。"""
    ensure_parent(file_path)
    if not append:
        tmp_path = build_part_path(file_path)
        if tmp_path.exists():
            tmp_path.unlink()
        with gzip.open(tmp_path, "wt", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        if not is_valid_gzip_file(tmp_path):
            tmp_path.unlink()
            raise RuntimeError(f"压缩文件校验失败: {file_path}")
        replace_output_file(tmp_path, file_path)
        return len(rows)
    mode = "at" if append else "wt"
    write_header = not append or not file_path.exists()
    with gzip.open(file_path, mode, encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)
    upload_file_to_s3(file_path)
    return len(rows)


def iter_zip_csv_lines(content: bytes):
    """遍历Zip内首个CSV文件的文本行。"""
    with zipfile.ZipFile(io_from_bytes(content), "r") as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            for line in f:
                yield line.decode("utf-8", errors="ignore").rstrip("\n")


def io_from_bytes(content: bytes):
    """将字节串包装为内存文件。"""
    return BytesIO(content)
