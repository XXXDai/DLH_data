from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import re
import sys
import zipfile

import app_config


DATA_ROOT = Path("data/src")  # 数据根目录，路径
DELIVERY_SYMBOL_PATTERN = re.compile(r".+-\d{2}[A-Z]{3}\d{2}$")  # 交割合约格式，正则


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
    return bool(DELIVERY_SYMBOL_PATTERN.match(symbol))


def validate_orderbook_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    allow_delivery: bool,
    start_date: str,
) -> None:
    if not data_dir.exists():
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    base_set = set(base_symbols)
    excludes = set(app_config.BYBIT_FUTURE_DELIVERY_EXCLUDE)  # 交割合约过滤列表，个数
    allowed_delivery_bases = base_set - excludes
    observed_symbols: set[str] = set()
    per_symbol_dates: dict[str, set[str]] = {}
    has_start_date = is_valid_ymd(start_date)

    for symbol_dir in sorted([p for p in data_dir.iterdir() if p.is_dir()]):
        symbol = symbol_dir.name
        observed_symbols.add(symbol)
        if allow_delivery and is_delivery_symbol(symbol):
            base = symbol.split("-", 1)[0]
            if base in excludes:
                report.error(f"{dataset_id} 发现被过滤交割合约: {symbol}")
            if base not in allowed_delivery_bases:
                report.error(f"{dataset_id} 发现未配置的交割合约基础交易对: {symbol}")
        else:
            if symbol not in base_set:
                report.error(f"{dataset_id} 发现未配置的交易对目录: {symbol}")

        dates = per_symbol_dates.setdefault(symbol, set())
        for file_path in sorted([p for p in symbol_dir.iterdir() if p.is_file()]):
            name = file_path.name
            if name.endswith(".part"):
                report.warn(f"{dataset_id} 发现临时文件，可能是未完成下载: {file_path}")
                continue
            suffix = f"_{symbol}_ob200.data.zip"
            if not name.endswith("_ob200.data.zip"):
                report.error(f"{dataset_id} 文件后缀不符合: {file_path}")
                continue
            if not name.endswith(suffix):
                report.error(f"{dataset_id} 文件名交易对不匹配: {file_path}")
                continue
            date_text = name[: -len(suffix)]
            if not is_valid_ymd(date_text):
                report.error(f"{dataset_id} 文件名日期不合法: {file_path}")
                continue
            if date_text in dates:
                report.error(f"{dataset_id} 同一日期重复文件: {file_path}")
            dates.add(date_text)
            if file_path.stat().st_size == 0:
                report.error(f"{dataset_id} 发现空文件: {file_path}")
            if not zipfile.is_zipfile(file_path):
                report.error(f"{dataset_id} 不是有效zip文件: {file_path}")
            if has_start_date and date_text < start_date:
                report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")

    if not observed_symbols:
        report.warn(f"{dataset_id} 数据目录为空: {data_dir}")
        return

    for sym in base_symbols:
        if sym not in observed_symbols:
            report.warn(f"{dataset_id} 缺少交易对目录: {sym}")


def validate_trade_di(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    base_symbols: list[str],
    allow_delivery: bool,
    start_date: str,
    file_prefix_style: str,
) -> None:
    if not data_dir.exists():
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    base_set = set(base_symbols)
    excludes = set(app_config.BYBIT_FUTURE_DELIVERY_EXCLUDE)  # 交割合约过滤列表，个数
    allowed_delivery_bases = base_set - excludes
    observed_symbols: set[str] = set()
    has_start_date = is_valid_ymd(start_date)

    for symbol_dir in sorted([p for p in data_dir.iterdir() if p.is_dir()]):
        symbol = symbol_dir.name
        observed_symbols.add(symbol)
        if allow_delivery and is_delivery_symbol(symbol):
            base = symbol.split("-", 1)[0]
            if base in excludes:
                report.error(f"{dataset_id} 发现被过滤交割合约: {symbol}")
            if base not in allowed_delivery_bases:
                report.error(f"{dataset_id} 发现未配置的交割合约基础交易对: {symbol}")
        else:
            if symbol not in base_set:
                report.error(f"{dataset_id} 发现未配置的交易对目录: {symbol}")

        for file_path in sorted([p for p in symbol_dir.iterdir() if p.is_file()]):
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
            if file_path.stat().st_size == 0:
                report.error(f"{dataset_id} 发现空文件: {file_path}")
            if has_start_date and date_text < start_date:
                report.warn(f"{dataset_id} 发现早于配置起始日期({start_date})的数据: {file_path}")

    if not observed_symbols:
        report.warn(f"{dataset_id} 数据目录为空: {data_dir}")
        return

    for sym in base_symbols:
        if sym not in observed_symbols:
            report.warn(f"{dataset_id} 缺少交易对目录: {sym}")


def validate_single_csv_per_symbol(
    report: Report,
    dataset_id: str,
    data_dir: Path,
    symbols: list[str],
    file_suffix: str,
) -> None:
    if not data_dir.exists():
        report.warn(f"{dataset_id} 数据目录不存在: {data_dir}")
        return

    observed: set[str] = set()
    for symbol_dir in sorted([p for p in data_dir.iterdir() if p.is_dir()]):
        symbol = symbol_dir.name
        observed.add(symbol)
        if symbols and symbol not in set(symbols):
            report.error(f"{dataset_id} 发现未配置的目录: {symbol}")
        file_path = symbol_dir / f"{symbol}{file_suffix}"
        if not file_path.exists():
            report.warn(f"{dataset_id} 缺少文件: {file_path}")
            continue
        if file_path.stat().st_size == 0:
            report.error(f"{dataset_id} 发现空文件: {file_path}")

    for sym in symbols:
        if sym not in observed:
            report.warn(f"{dataset_id} 缺少目录: {sym}")


def main() -> int:
    report = Report()

    base_symbols = app_config.parse_bybit_symbols(app_config.BYBIT_SYMBOL)
    if not base_symbols:
        print("未配置 BYBIT_SYMBOL，无法校验。")
        return 2

    validate_orderbook_di(
        report,
        "D10001",
        DATA_ROOT / "bybit_future_orderbook_di",
        base_symbols,
        True,
        app_config.D10001_START_DATE,
    )
    validate_orderbook_di(
        report,
        "D10005",
        DATA_ROOT / "bybit_spot_orderbook_di",
        base_symbols,
        False,
        app_config.D10005_START_DATE,
    )
    validate_trade_di(
        report,
        "D10013",
        DATA_ROOT / "bybit_future_trade_di",
        base_symbols,
        True,
        app_config.D10013_START_DATE,
        "concat",
    )
    validate_trade_di(
        report,
        "D10014",
        DATA_ROOT / "bybit_spot_trade_di",
        base_symbols,
        False,
        app_config.D10014_START_DATE,
        "underscore",
    )
    validate_single_csv_per_symbol(
        report,
        "D10017",
        DATA_ROOT / "bybit_future_fundingrate_di",
        base_symbols,
        "_fundingrate.csv",
    )
    validate_single_csv_per_symbol(
        report,
        "D10018",
        DATA_ROOT / "bybit_insurance_di",
        app_config.BYBIT_INSURANCE_COINS,
        "_insurance.csv",
    )
    validate_single_csv_per_symbol(
        report,
        "D10019",
        DATA_ROOT / "bybit_onchainstaking_di",
        app_config.BYBIT_ONCHAIN_COINS,
        "_onchainstaking.csv",
    )

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
