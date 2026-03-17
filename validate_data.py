from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
import sys
import tarfile
import tempfile
import zipfile

import app_config
from cex import cex_common
from cex import cex_config


DATA_ROOT = Path("data/src")  # 数据根目录，路径
DELIVERY_SYMBOL_PATTERN = re.compile(r".+-\d{2}[A-Z]{3}\d{2}$")  # 交割合约格式，正则
OKX_DELIVERY_SYMBOL_PATTERN = re.compile(r".+-\d{6}$")  # OKX交割合约格式，正则
BITGET_ARCHIVE_NAME_PATTERN = re.compile(r"^(\d{8})_\d{3}\.zip$")  # Bitget归档文件名格式，正则
VALIDATE_CACHE_ROOT: Path | None = None  # 远端校验临时缓存目录，路径
VALIDATE_S3_KEYS: set[str] | None = None  # S3对象键缓存集合，个数
VALIDATE_S3_FILE_SIZES: dict[str, int] | None = None  # S3对象大小映射，字节
VALIDATE_S3_CHILD_DIRS: dict[str, list[str]] | None = None  # S3子目录映射，映射
VALIDATE_S3_CHILD_FILES: dict[str, list[str]] | None = None  # S3子文件映射，映射


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    infos: list[str] = field(default_factory=list)

    def error(self, message: str) -> None:
        self.errors.append(message)

    def warn(self, message: str) -> None:
        self.warnings.append(message)

    def info(self, message: str) -> None:
        self.infos.append(message)


def apply_storage_mode_from_argv() -> None:
    """根据启动参数设置校验存储模式。"""
    app_config.DATA_STORAGE_MODE = "s3" if "-s3" in sys.argv else "local"


def build_cache_path(storage_path: Path) -> Path:
    """将存储路径映射到本地校验缓存路径。"""
    if VALIDATE_CACHE_ROOT is None:
        raise RuntimeError("校验缓存目录未初始化")
    relative_path = storage_path.relative_to(cex_config.DATA_DYLAN_ROOT)
    return VALIDATE_CACHE_ROOT / relative_path


def build_progress_bar(current: int, total: int, width: int = 28) -> str:
    """构造命令行进度条文本。"""
    safe_total = max(1, total)
    filled = min(width, int(width * current / safe_total))
    return f"[{'#' * filled}{'-' * (width - filled)}]"


def print_progress(prefix: str, current: int, total: int, detail: str) -> None:
    """打印单行进度信息。"""
    progress_text = f"\r{prefix} {build_progress_bar(current, total)} {current}/{total} {detail}"
    print(progress_text[:180], end="", flush=True)
    if current >= total:
        print()


def print_scan_progress(prefix: str, current: int, detail: str) -> None:
    """打印扫描阶段进度信息。"""
    progress_text = f"\r{prefix} 已发现 {current} 个文件 {detail}"
    print(progress_text[:180], end="", flush=True)


def build_s3_key_prefix(path: Path) -> str:
    """构造目录或文件对应的S3键前缀。"""
    absolute_path = path.resolve()
    data_root = cex_config.DATA_DYLAN_ROOT.resolve()
    relative_path = absolute_path.relative_to(data_root)
    return f"{app_config.S3_PREFIX}/{relative_path.as_posix()}"


def build_storage_relative_path(path: Path) -> str:
    """构造相对数据根目录的路径字符串。"""
    absolute_path = path.resolve()
    data_root = cex_config.DATA_DYLAN_ROOT.resolve()
    return absolute_path.relative_to(data_root).as_posix()


def print_dataset_progress(label: str, current: int, total: int, detail: str) -> None:
    """打印当前数据集内部进度。"""
    progress_text = f"\r当前数据集 {label} {build_progress_bar(current, total)} {current}/{total} {detail}"
    print(progress_text[:180], end="", flush=True)
    if current >= total:
        print()


def prefetch_storage_key_index() -> None:
    """预拉取S3对象键索引。"""
    global VALIDATE_S3_KEYS
    global VALIDATE_S3_FILE_SIZES
    global VALIDATE_S3_CHILD_DIRS
    global VALIDATE_S3_CHILD_FILES
    if not cex_common.is_s3_storage_mode():
        VALIDATE_S3_KEYS = None
        VALIDATE_S3_FILE_SIZES = None
        VALIDATE_S3_CHILD_DIRS = None
        VALIDATE_S3_CHILD_FILES = None
        return
    prefix = f"{app_config.S3_PREFIX}/"
    keys = set()
    file_sizes = {}
    child_dirs: dict[str, set[str]] = {}
    child_files: dict[str, list[str]] = {}
    paginator = cex_common.get_s3_client().get_paginator("list_objects_v2")
    last_name = "-"
    last_reported = 0
    for page in paginator.paginate(
        Bucket=app_config.S3_BUCKET_NAME,
        Prefix=prefix,
        PaginationConfig={"PageSize": 1000},
    ):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key or key.endswith("/"):
                continue
            keys.add(key)
            file_sizes[key] = int(item.get("Size") or 0)
            suffix = key[len(prefix) :] if key.startswith(prefix) else key
            last_name = Path(suffix).name
            parts = suffix.split("/")
            for index, dir_name in enumerate(parts[:-1]):
                parent_rel = "/".join(parts[:index])
                child_dirs.setdefault(parent_rel, set()).add(dir_name)
            parent_rel = "/".join(parts[:-1])
            child_files.setdefault(parent_rel, []).append(parts[-1])
        if len(keys) - last_reported >= 5000:
            print_scan_progress("索引S3文件", len(keys), last_name)
            last_reported = len(keys)
    print_scan_progress("索引S3文件", len(keys), last_name)
    print()
    VALIDATE_S3_KEYS = keys
    VALIDATE_S3_FILE_SIZES = file_sizes
    VALIDATE_S3_CHILD_DIRS = {key: sorted(values) for key, values in child_dirs.items()}
    VALIDATE_S3_CHILD_FILES = {key: sorted(values) for key, values in child_files.items()}


def storage_dir_exists(dir_path: Path) -> bool:
    """判断目录在当前存储模式下是否存在。"""
    if cex_common.is_s3_storage_mode() and VALIDATE_S3_CHILD_DIRS is not None and VALIDATE_S3_CHILD_FILES is not None:
        relative_path = build_storage_relative_path(dir_path)
        return relative_path in VALIDATE_S3_CHILD_DIRS or relative_path in VALIDATE_S3_CHILD_FILES
    if not cex_common.is_s3_storage_mode():
        return dir_path.exists()
    prefix = cex_common.build_s3_prefix(dir_path)
    if not prefix:
        return False
    paginator = cex_common.get_s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=app_config.S3_BUCKET_NAME, Prefix=prefix, PaginationConfig={"MaxItems": 1}):
        if page.get("Contents"):
            return True
    return False


def iter_storage_dirs(dir_path: Path) -> list[Path]:
    """列出当前存储模式下的直接子目录。"""
    if cex_common.is_s3_storage_mode() and VALIDATE_S3_CHILD_DIRS is not None:
        relative_path = build_storage_relative_path(dir_path)
        return [dir_path / name for name in VALIDATE_S3_CHILD_DIRS.get(relative_path, [])]
    if not cex_common.is_s3_storage_mode():
        if not dir_path.exists():
            return []
        return sorted([path for path in dir_path.iterdir() if path.is_dir()], key=lambda path: path.name)
    prefix = cex_common.build_s3_prefix(dir_path)
    if not prefix:
        return []
    names = set()
    paginator = cex_common.get_s3_client().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=app_config.S3_BUCKET_NAME, Prefix=prefix):
        for item in page.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.startswith(prefix) or key.endswith("/"):
                continue
            suffix = key[len(prefix) :]
            if "/" not in suffix:
                continue
            names.add(suffix.split("/", 1)[0])
    return [dir_path / name for name in sorted(names)]


def iter_storage_files(dir_path: Path) -> list[Path]:
    """列出当前存储模式下的直接子文件。"""
    if cex_common.is_s3_storage_mode() and VALIDATE_S3_CHILD_FILES is not None:
        relative_path = build_storage_relative_path(dir_path)
        return [dir_path / name for name in VALIDATE_S3_CHILD_FILES.get(relative_path, [])]
    return [dir_path / name for name in cex_common.list_storage_file_names(dir_path)]


def storage_file_exists(file_path: Path) -> bool:
    """判断文件在当前存储模式下是否存在。"""
    if cex_common.is_s3_storage_mode() and VALIDATE_S3_FILE_SIZES is not None:
        return build_s3_key_prefix(file_path) in VALIDATE_S3_FILE_SIZES
    return cex_common.storage_file_exists(file_path)


def storage_file_size(file_path: Path) -> int:
    """返回文件在当前存储模式下的大小。"""
    if cex_common.is_s3_storage_mode() and VALIDATE_S3_FILE_SIZES is not None:
        return int(VALIDATE_S3_FILE_SIZES.get(build_s3_key_prefix(file_path), 0))
    return file_path.stat().st_size


def materialize_storage_file(file_path: Path) -> Path:
    """将当前存储模式下的文件准备到本地供校验使用。"""
    if not cex_common.is_s3_storage_mode():
        return file_path
    local_path = build_cache_path(file_path)
    if local_path.exists():
        return local_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    s3_key = cex_common.build_s3_key(file_path)
    if not s3_key:
        raise RuntimeError(f"S3对象键构造失败: {file_path}")
    cex_common.get_s3_client().download_file(
        app_config.S3_BUCKET_NAME,
        s3_key,
        str(local_path),
        Config=cex_common.get_s3_transfer_config(),
    )
    return local_path


def is_valid_ymd(date_text: str) -> bool:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_text):
        return False
    year = int(date_text[0:4])
    month = int(date_text[5:7])
    day = int(date_text[8:10])
    if month < 1 or month > 12:
        return False
    if day < 1:
        return False
    is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
    days_in_month = [31, 29 if is_leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    return day <= days_in_month[month - 1]


def is_delivery_symbol(symbol: str) -> bool:
    """判断是否为交割合约目录名。"""
    return bool(DELIVERY_SYMBOL_PATTERN.match(symbol) or OKX_DELIVERY_SYMBOL_PATTERN.match(symbol))


def delivery_base_symbol(symbol: str) -> str:
    """提取交割合约对应的基础标识。"""
    if DELIVERY_SYMBOL_PATTERN.match(symbol):
        return symbol.split("-", 1)[0]
    if OKX_DELIVERY_SYMBOL_PATTERN.match(symbol):
        return "-".join(symbol.split("-")[:2])
    return symbol


def validate_orderbook_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    allow_delivery: bool,
    start_date: str,
    exchange: str,
) -> None:
    """校验历史订单簿归档目录。"""
    if not storage_dir_exists(data_dir):
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    base_set = set(base_symbols)
    excludes = set(cex_config.BYBIT_FUTURE_DELIVERY_EXCLUDE)  # 交割合约过滤列表，个数
    allowed_delivery_bases = base_set - excludes
    observed_symbols: set[str] = set()
    per_symbol_dates: dict[str, set[str]] = {}
    has_start_date = is_valid_ymd(start_date)
    symbol_dirs = [path for path in iter_storage_dirs(data_dir) if not path.name.startswith("__")]
    total_files = sum(len(iter_storage_files(symbol_dir)) for symbol_dir in symbol_dirs)
    processed_files = 0

    for symbol_dir in symbol_dirs:
        symbol = symbol_dir.name
        observed_symbols.add(symbol)
        if allow_delivery and is_delivery_symbol(symbol):
            base = delivery_base_symbol(symbol)
            if base in excludes:
                report.error(f"{dataset_id} 发现被过滤交割合约: {symbol}")
            if base not in allowed_delivery_bases:
                report.error(f"{dataset_id} 发现未配置的交割合约基础交易对: {symbol}")
        else:
            if symbol not in base_set:
                report.error(f"{dataset_id} 发现未配置的交易对目录: {symbol}")

        dates = per_symbol_dates.setdefault(symbol, set())
        for file_path in iter_storage_files(symbol_dir):
            processed_files += 1
            print_dataset_progress(dataset_id, processed_files, total_files, file_path.name)
            name = file_path.name
            if name.endswith(".part"):
                report.warn(f"{dataset_id} 发现临时文件，可能是未完成下载: {file_path}")
                continue
            if exchange == "bybit":
                suffix = f"_{symbol}_ob200.data.zip"
                if not name.endswith("_ob200.data.zip"):
                    report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                    continue
                if not name.endswith(suffix):
                    report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                    continue
                date_text = name[: -len(suffix)]
            elif exchange == "binance":
                prefix = f"{symbol}-bookTicker-"
                suffix = ".zip"
                if not name.endswith(suffix):
                    report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                    continue
                if not name.startswith(prefix):
                    report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                    continue
                date_text = name[len(prefix) : -len(suffix)]
            elif exchange == "bitget":
                if not name.endswith(".zip"):
                    report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                    continue
                raw_date = name.removesuffix(".zip")
                if len(raw_date) != 8 or not raw_date.isdigit():
                    report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                    continue
                date_text = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
            elif exchange == "okx":
                suffix = f"_{symbol}_ob400.data.zip"
                if not name.endswith(suffix):
                    report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                    continue
                date_text = name[: -len(suffix)]
            else:
                report.error(f"{dataset_id} 校验脚本配置错误: exchange={exchange}")
                return
            if not is_valid_ymd(date_text):
                report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                continue
            if date_text in dates:
                report.error(f"{dataset_id} 同一日期重复文件: {file_path}")
            dates.add(date_text)
            file_size = storage_file_size(file_path)
            if file_size == 0:
                report.error(f"{dataset_id} 发现空文件: {file_path}")
            if not cex_common.is_s3_storage_mode():
                local_file_path = materialize_storage_file(file_path)
                if exchange == "bybit" and not zipfile.is_zipfile(local_file_path):
                    report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
                if exchange == "binance" and not zipfile.is_zipfile(local_file_path):
                    report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
                if exchange == "bitget" and not zipfile.is_zipfile(local_file_path):
                    report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
                if exchange == "okx" and not zipfile.is_zipfile(local_file_path):
                    report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
            if has_start_date and date_text < start_date:
                report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")

    if not observed_symbols:
        report.warn(f"{dataset_id} 数据目录为空: {data_dir}")
        return

    for sym in base_symbols:
        if sym not in observed_symbols:
            report.warn(f"{dataset_id} 缺少交易对目录: {data_dir / sym}")


def validate_trade_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    allow_delivery: bool,
    start_date: str,
    file_prefix_style: str,
) -> None:
    """校验成交归档目录。"""
    if not storage_dir_exists(data_dir):
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    base_set = set(base_symbols)
    excludes = set(cex_config.BYBIT_FUTURE_DELIVERY_EXCLUDE)  # 交割合约过滤列表，个数
    allowed_delivery_bases = base_set - excludes
    observed_symbols: set[str] = set()
    has_start_date = is_valid_ymd(start_date)
    symbol_dirs = [path for path in iter_storage_dirs(data_dir) if not path.name.startswith("__")]
    total_files = sum(len(iter_storage_files(symbol_dir)) for symbol_dir in symbol_dirs)
    processed_files = 0

    for symbol_dir in symbol_dirs:
        symbol = symbol_dir.name
        observed_symbols.add(symbol)
        if allow_delivery and is_delivery_symbol(symbol):
            base = delivery_base_symbol(symbol)
            if base in excludes:
                report.error(f"{dataset_id} 发现被过滤交割合约: {symbol}")
            if base not in allowed_delivery_bases:
                report.error(f"{dataset_id} 发现未配置的交割合约基础交易对: {symbol}")
        else:
            if symbol not in base_set:
                report.error(f"{dataset_id} 发现未配置的交易对目录: {symbol}")

        for file_path in iter_storage_files(symbol_dir):
            processed_files += 1
            print_dataset_progress(dataset_id, processed_files, total_files, file_path.name)
            name = file_path.name
            if name.endswith(".part"):
                report.warn(f"{dataset_id} 发现临时文件，可能是未完成下载: {file_path}")
                continue
            if not name.endswith(".csv.gz"):
                report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                continue
            date_text = ""
            if file_prefix_style == "concat":
                if not name.startswith(symbol) or not name.endswith(".csv.gz"):
                    report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                    continue
                date_text = name[len(symbol) : -len(".csv.gz")]
            elif file_prefix_style == "underscore":
                prefix = f"{symbol}_"
                if not name.startswith(prefix) or not name.endswith(".csv.gz"):
                    report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                    continue
                date_text = name[len(prefix) : -len(".csv.gz")]
            else:
                report.error(f"{dataset_id} 校验脚本配置错误: file_prefix_style={file_prefix_style}")
                return

            if not is_valid_ymd(date_text):
                report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                continue
            if storage_file_size(file_path) == 0:
                report.error(f"{dataset_id} 发现空文件: {file_path}")
            if has_start_date and date_text < start_date:
                report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")

    if not observed_symbols:
        report.warn(f"{dataset_id} 数据目录为空: {data_dir}")
        return

    for sym in base_symbols:
        if sym not in observed_symbols:
            report.warn(f"{dataset_id} 缺少交易对目录: {data_dir / sym}")


def parse_bitget_archive_date(name: str) -> str | None:
    """解析Bitget原始归档日期。"""
    matched = BITGET_ARCHIVE_NAME_PATTERN.match(name)
    if not matched:
        return None
    raw_date = matched.group(1)
    return f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"


def validate_bitget_trade_raw_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    allow_delivery: bool,
    start_date: str,
    file_prefix_style: str,
) -> None:
    """校验Bitget成交目录。"""
    if not storage_dir_exists(data_dir):
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    base_set = set(base_symbols)
    excludes = set(cex_config.BYBIT_FUTURE_DELIVERY_EXCLUDE)  # 交割合约过滤列表，个数
    allowed_delivery_bases = base_set - excludes
    observed_symbols: set[str] = set()
    has_start_date = is_valid_ymd(start_date)
    symbol_dirs = [path for path in iter_storage_dirs(data_dir) if not path.name.startswith("__")]
    total_files = sum(len(iter_storage_files(symbol_dir)) for symbol_dir in symbol_dirs)
    processed_files = 0

    for symbol_dir in symbol_dirs:
        symbol = symbol_dir.name
        observed_symbols.add(symbol)
        if allow_delivery and is_delivery_symbol(symbol):
            base = delivery_base_symbol(symbol)
            if base in excludes:
                report.error(f"{dataset_id} 发现被过滤交割合约: {symbol}")
            if base not in allowed_delivery_bases:
                report.error(f"{dataset_id} 发现未配置的交割合约基础交易对: {symbol}")
        else:
            if symbol not in base_set:
                report.error(f"{dataset_id} 发现未配置的交易对目录: {symbol}")

        for file_path in iter_storage_files(symbol_dir):
            processed_files += 1
            print_dataset_progress(dataset_id, processed_files, total_files, file_path.name)
            name = file_path.name
            if name.endswith(".part"):
                report.warn(f"{dataset_id} 发现临时文件，可能是未完成下载: {file_path}")
                continue
            if name.endswith(".csv.gz"):
                if file_prefix_style == "concat":
                    if not name.startswith(symbol) or not name.endswith(".csv.gz"):
                        report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                        continue
                    date_text = name[len(symbol) : -len(".csv.gz")]
                else:
                    prefix = f"{symbol}_"
                    if not name.startswith(prefix) or not name.endswith(".csv.gz"):
                        report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                        continue
                    date_text = name[len(prefix) : -len(".csv.gz")]
                if not is_valid_ymd(date_text):
                    report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                    continue
                if storage_file_size(file_path) == 0:
                    report.error(f"{dataset_id} 发现空文件: {file_path}")
                if has_start_date and date_text < start_date:
                    report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")
                continue
            date_text = parse_bitget_archive_date(name)
            if not date_text:
                report.error(f"{dataset_id} 文件名不符合Bitget成交格式: {file_path}")
                continue
            if not is_valid_ymd(date_text):
                report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                continue
            if storage_file_size(file_path) == 0:
                report.error(f"{dataset_id} 发现空文件: {file_path}")
            if cex_common.is_s3_storage_mode():
                report.warn(f"{dataset_id} 发现旧版Bitget zip归档: {file_path}")
            else:
                local_file_path = materialize_storage_file(file_path)
                if not zipfile.is_zipfile(local_file_path):
                    report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
                else:
                    report.warn(f"{dataset_id} 发现旧版Bitget zip归档: {file_path}")
            if has_start_date and date_text < start_date:
                report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")

    if not observed_symbols:
        report.warn(f"{dataset_id} 数据目录为空: {data_dir}")
        return

    for sym in base_symbols:
        if sym not in observed_symbols:
            report.warn(f"{dataset_id} 缺少交易对目录: {data_dir / sym}")


def validate_single_csv_per_symbol(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    symbols: list[str],
    file_suffix: str,
) -> None:
    """校验每个目录仅一份CSV的数据集。"""
    if not storage_dir_exists(data_dir):
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    observed: set[str] = set()
    symbol_set = set(symbols)
    symbol_dirs = [path for path in iter_storage_dirs(data_dir) if not path.name.startswith("__")]
    total_symbols = len(symbol_dirs)
    processed_symbols = 0
    for symbol_dir in symbol_dirs:
        processed_symbols += 1
        print_dataset_progress(dataset_id, processed_symbols, total_symbols, symbol_dir.name)
        symbol = symbol_dir.name
        observed.add(symbol)
        if symbols and symbol not in symbol_set:
            report.error(f"{dataset_id} 发现未配置的目录: {symbol}")
        file_path = symbol_dir / f"{symbol}{file_suffix}"
        if not storage_file_exists(file_path):
            report.warn(f"{dataset_id} 缺少文件: {file_path}")
            continue
        if storage_file_size(file_path) == 0:
            report.error(f"{dataset_id} 发现空文件: {file_path}")

    for sym in symbols:
        if sym not in observed:
            report.warn(f"{dataset_id} 缺少目录: {data_dir / sym}")


def validate_enabled_orderbook_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    has_delivery: bool,
    start_date: str,
    exchange: str,
) -> None:
    """仅校验已启用交易所的订单簿目录。"""
    base_dataset_id = dataset_id.split("/", 1)[0]
    if not cex_config.is_supported(base_dataset_id, exchange):
        return
    validate_orderbook_di(report, dataset_id, data_dir, base_symbols, has_delivery, start_date, exchange)


def run_validation_step(step_state: dict, total_steps: int, label: str) -> None:
    """推进校验阶段进度。"""
    step_state["current"] += 1
    print_progress("执行校验", step_state["current"], total_steps, label)


def main() -> int:
    """执行全量数据校验。"""
    global VALIDATE_CACHE_ROOT
    global VALIDATE_S3_KEYS
    apply_storage_mode_from_argv()
    report = Report()
    with tempfile.TemporaryDirectory(prefix="validate_data_") as temp_dir:
        VALIDATE_CACHE_ROOT = Path(temp_dir)
        prefetch_storage_key_index()
        total_steps = 7 + len(cex_config.list_exchanges()) * 2 + len(cex_config.get_supported_exchanges("D10017")) + len(cex_config.get_supported_exchanges("D10018")) + len(cex_config.get_supported_exchanges("D10019"))
        step_state = {"current": 0}
        run_validation_step(step_state, total_steps, "D10001/bybit")
        validate_enabled_orderbook_di(
            report,
            "D10001/bybit",
            cex_config.get_source_dir("D10001", "bybit") or DATA_ROOT / "bybit_future_orderbook_di",
            cex_config.get_future_symbols("bybit"),
            True,
            cex_config.get_min_start_date("D10001", "bybit"),
            "bybit",
        )
        run_validation_step(step_state, total_steps, "D10001/okx")
        validate_enabled_orderbook_di(
            report,
            "D10001/okx",
            cex_config.get_source_dir("D10001", "okx") or DATA_ROOT / "okx_future_orderbook_di",
            cex_config.get_future_symbols("okx"),
            False,
            cex_config.get_min_start_date("D10001", "okx"),
            "okx",
        )
        run_validation_step(step_state, total_steps, "D10001/binance")
        validate_enabled_orderbook_di(
            report,
            "D10001/binance",
            cex_config.get_source_dir("D10001", "binance") or DATA_ROOT / "binance_future_orderbook_di",
            cex_config.get_future_symbols("binance"),
            False,
            cex_config.get_min_start_date("D10001", "binance"),
            "binance",
        )
        run_validation_step(step_state, total_steps, "D10001/bitget")
        validate_enabled_orderbook_di(
            report,
            "D10001/bitget",
            cex_config.get_source_dir("D10001", "bitget") or DATA_ROOT / "bitget_future_orderbook_di",
            cex_config.get_future_symbols("bitget"),
            False,
            cex_config.get_min_start_date("D10001", "bitget"),
            "bitget",
        )
        run_validation_step(step_state, total_steps, "D10005/bybit")
        validate_enabled_orderbook_di(
            report,
            "D10005/bybit",
            cex_config.get_source_dir("D10005", "bybit") or DATA_ROOT / "bybit_spot_orderbook_di",
            cex_config.get_spot_symbols("bybit"),
            False,
            cex_config.get_min_start_date("D10005", "bybit"),
            "bybit",
        )
        run_validation_step(step_state, total_steps, "D10005/okx")
        validate_enabled_orderbook_di(
            report,
            "D10005/okx",
            cex_config.get_source_dir("D10005", "okx") or DATA_ROOT / "okx_spot_orderbook_di",
            cex_config.get_spot_symbols("okx"),
            False,
            cex_config.get_min_start_date("D10005", "okx"),
            "okx",
        )
        run_validation_step(step_state, total_steps, "D10005/bitget")
        validate_enabled_orderbook_di(
            report,
            "D10005/bitget",
            cex_config.get_source_dir("D10005", "bitget") or DATA_ROOT / "bitget_spot_orderbook_di",
            cex_config.get_spot_symbols("bitget"),
            False,
            cex_config.get_min_start_date("D10005", "bitget"),
            "bitget",
        )
        for exchange in cex_config.list_exchanges():
            run_validation_step(step_state, total_steps, f"D10013/{exchange}")
            data_dir = cex_config.get_source_dir("D10013", exchange)
            if not data_dir:
                continue
            if exchange == "bitget":
                validate_bitget_trade_raw_di(
                    report,
                    f"D10013/{exchange}",
                    data_dir,
                    sorted(set(cex_config.get_future_trade_symbols(exchange)) | set(cex_config.get_delivery_families(exchange))),
                    exchange in {"bybit", "okx"},
                    cex_config.get_min_start_date("D10013", exchange),
                    "concat",
                )
            else:
                validate_trade_di(
                    report,
                    f"D10013/{exchange}",
                    data_dir,
                    sorted(set(cex_config.get_future_trade_symbols(exchange)) | set(cex_config.get_delivery_families(exchange))),
                    exchange in {"bybit", "okx"},
                    cex_config.get_min_start_date("D10013", exchange),
                    "concat",
                )
        for exchange in cex_config.list_exchanges():
            run_validation_step(step_state, total_steps, f"D10014/{exchange}")
            data_dir = cex_config.get_source_dir("D10014", exchange)
            if not data_dir:
                continue
            if exchange == "bitget":
                validate_bitget_trade_raw_di(
                    report,
                    f"D10014/{exchange}",
                    data_dir,
                    cex_config.get_spot_trade_symbols(exchange),
                    False,
                    cex_config.get_min_start_date("D10014", exchange),
                    "underscore",
                )
            else:
                validate_trade_di(
                    report,
                    f"D10014/{exchange}",
                    data_dir,
                    cex_config.get_spot_trade_symbols(exchange),
                    False,
                    cex_config.get_min_start_date("D10014", exchange),
                    "underscore",
                )
        for exchange in cex_config.get_supported_exchanges("D10017"):
            run_validation_step(step_state, total_steps, f"D10017/{exchange}")
            data_dir = cex_config.get_source_dir("D10017", exchange)
            if not data_dir:
                continue
            validate_single_csv_per_symbol(
                report,
                f"D10017/{exchange}",
                data_dir,
                cex_config.get_funding_symbols(exchange),
                "_fundingrate.csv",
            )
        for exchange in cex_config.get_supported_exchanges("D10018"):
            run_validation_step(step_state, total_steps, f"D10018/{exchange}")
            data_dir = cex_config.get_source_dir("D10018", exchange)
            if not data_dir:
                continue
            validate_single_csv_per_symbol(
                report,
                f"D10018/{exchange}",
                data_dir,
                cex_config.get_insurance_symbols(exchange),
                "_insurance.csv",
            )
        for exchange in cex_config.get_supported_exchanges("D10019"):
            run_validation_step(step_state, total_steps, f"D10019/{exchange}")
            data_dir = cex_config.get_source_dir("D10019", exchange)
            if not data_dir:
                continue
            validate_single_csv_per_symbol(
                report,
                f"D10019/{exchange}",
                data_dir,
                cex_config.get_earn_coins(exchange),
                "_onchainstaking.csv",
            )
    VALIDATE_CACHE_ROOT = None
    VALIDATE_S3_KEYS = None

    print("数据校验结果")
    print(f"错误: {len(report.errors)}")
    print(f"警告: {len(report.warnings)}")
    print(f"信息: {len(report.infos)}")

    if report.errors:
        print("\n错误详情")
        for line in report.errors:
            print(f"- {line}")
    if report.warnings:
        print("\n警告详情")
        for line in report.warnings:
            print(f"- {line}")
    if report.infos:
        print("\n信息详情")
        for line in report.infos:
            print(f"- {line}")

    return 1 if report.errors else 0


if __name__ == "__main__":
    sys.exit(main())
