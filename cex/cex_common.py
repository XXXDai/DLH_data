from functools import lru_cache
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path
import csv
import gzip
import json
import socket
import threading
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ConnectTimeoutError
from botocore.exceptions import EndpointConnectionError
from botocore.exceptions import NoCredentialsError
from botocore.exceptions import PartialCredentialsError
from botocore.exceptions import ReadTimeoutError

import app_config
from cex import cex_config


PART_FILE_STALE_SECONDS = 30 * 60  # 临时文件过期时间，秒
FAILURE_FILE_LOCK = threading.Lock()  # 失败记录文件写入锁，锁对象


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
        ),
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
    try:
        get_s3_client().download_file(app_config.S3_BUCKET_NAME, s3_key, str(tmp_path))
    except NoCredentialsError as exc:
        raise RuntimeError("S3下载失败: 缺少凭证") from exc
    except PartialCredentialsError as exc:
        raise RuntimeError("S3下载失败: 凭证不完整") from exc
    except ConnectTimeoutError as exc:
        raise RuntimeError("S3下载失败: 连接超时") from exc
    except ReadTimeoutError as exc:
        raise RuntimeError("S3下载失败: 读取超时") from exc
    except EndpointConnectionError as exc:
        raise RuntimeError("S3下载失败: 无法连接S3端点") from exc
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise RuntimeError(f"S3下载失败: {code or '未知错误'}") from exc
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
    """上传单个本地文件到S3。"""
    if not is_s3_storage_mode():
        return
    if not file_path.exists() or not file_path.is_file():
        return
    s3_key = build_s3_key(file_path)
    if not s3_key:
        return
    try:
        get_s3_client().upload_file(str(file_path), app_config.S3_BUCKET_NAME, s3_key)
    except NoCredentialsError as exc:
        raise RuntimeError("S3上传失败: 缺少凭证") from exc
    except PartialCredentialsError as exc:
        raise RuntimeError("S3上传失败: 凭证不完整") from exc
    except ConnectTimeoutError as exc:
        raise RuntimeError("S3上传失败: 连接超时") from exc
    except ReadTimeoutError as exc:
        raise RuntimeError("S3上传失败: 读取超时") from exc
    except EndpointConnectionError as exc:
        raise RuntimeError("S3上传失败: 无法连接S3端点") from exc
    except ClientError as exc:
        raise RuntimeError(f"S3上传失败: {exc.response.get('Error', {}).get('Code', '未知错误')}") from exc


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
