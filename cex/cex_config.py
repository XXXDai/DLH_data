from pathlib import Path
import threading
import time


CEX_EXCHANGES = ["bybit", "binance", "bitget", "okx"]  # CEX交易所列表，个数
CEX_BASE_COINS = ["BTC", "ETH", "SOL"]  # 基础币种列表，个数
EXCHANGE_ENABLED = {
    "bybit": False,  # Bybit总开关，开关
    "binance": True,  # Binance总开关，开关
    "bitget": True,  # Bitget总开关，开关
    "okx": True,  # OKX总开关，开关
}  # 交易所总开关映射，映射

SPOT_SYMBOLS = {
    "bybit": ["AAPLXUSDT", "AMZNXUSDT", "COINXUSDT", "CRCLXUSDT", "GOOGLXUSDT", "HOODXUSDT", "MCDXUSDT", "METAXUSDT", "NVDAXUSDT", "TSLAXUSDT"],  # Bybit现货交易对列表，个数
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

BYBIT_FUTURE_DELIVERY_CATEGORIES = ["linear"]  # Bybit交割合约产品类型列表，个数
BYBIT_FUTURE_DELIVERY_STATUSES = ["Trading", "PreLaunch", "Delivering", "Closed"]  # Bybit交割合约状态列表，个数
BYBIT_FUTURE_DELIVERY_EXCLUDE = ["MNTUSDT"]  # Bybit交割合约过滤列表，个数
BYBIT_XSTOCK_SPOT_TRADE_SYMBOLS = [  # Bybit美股代币现货成交交易对列表，个数
    "AAPLXUSDT",
    "AMZNXUSDT",
    "COINXUSDT",
    "CRCLXUSDT",
    "GOOGLXUSDT",
    "HOODXUSDT",
    "MCDXUSDT",
    "METAXUSDT",
    "NVDAXUSDT",
    "TSLAXUSDT",
]
BINANCE_XSTOCK_FUTURE_SYMBOLS = [  # Binance美股代币合约交易对列表，个数
    "AMZNUSDT",
    "COINUSDT",
    "CRCLUSDT",
    "HOODUSDT",
    "INTCUSDT",
    "MSTRUSDT",
    "PLTRUSDT",
    "TSLAUSDT",
]
BITGET_XSTOCK_FUTURE_SYMBOLS = [  # Bitget美股代币合约交易对列表，个数
    "AAPLUSDT",
    "ACNUSDT",
    "AMZNUSDT",
    "APPUSDT",
    "ARMUSDT",
    "ASMLUSDT",
    "AVGOUSDT",
    "BABAUSDT",
    "COINUSDT",
    "COSTUSDT",
    "CRCLUSDT",
    "CSCOUSDT",
    "FUTUUSDT",
    "GEUSDT",
    "GMEUSDT",
    "GOOGLUSDT",
    "HOODUSDT",
    "IBMUSDT",
    "INTCUSDT",
    "JDUSDT",
    "LLYUSDT",
    "MAUSDT",
    "MCDUSDT",
    "METAUSDT",
    "MRVLUSDT",
    "MSFTUSDT",
    "MSTRUSDT",
    "MUUSDT",
    "NVDAUSDT",
    "ORCLUSDT",
    "PEPUSDT",
    "PLTRUSDT",
    "QQQUSDT",
    "RDDTUSDT",
    "SPYUSDT",
    "TSLAUSDT",
    "TSMUSDT",
    "UNHUSDT",
    "WMTUSDT",
]
OKX_XSTOCK_FUTURE_SYMBOLS = [  # OKX美股代币合约交易对列表，个数
    "AAPL-USDT-SWAP",
    "AMD-USDT-SWAP",
    "AMZN-USDT-SWAP",
    "COIN-USDT-SWAP",
    "CRCL-USDT-SWAP",
    "EWY-USDT-SWAP",
    "GOOGL-USDT-SWAP",
    "HOOD-USDT-SWAP",
    "INTC-USDT-SWAP",
    "META-USDT-SWAP",
    "MSFT-USDT-SWAP",
    "MSTR-USDT-SWAP",
    "MU-USDT-SWAP",
    "NFLX-USDT-SWAP",
    "NVDA-USDT-SWAP",
    "ORCL-USDT-SWAP",
    "PLTR-USDT-SWAP",
    "QQQ-USDT-SWAP",
    "SNDK-USDT-SWAP",
    "SPY-USDT-SWAP",
    "TSLA-USDT-SWAP",
]
XSTOCK_SPOT_TRADE_SYMBOLS = {
    "bybit": BYBIT_XSTOCK_SPOT_TRADE_SYMBOLS,  # Bybit美股代币现货成交交易对列表，个数
    "binance": [],  # Binance美股代币现货成交交易对列表，个数
    "bitget": [],  # Bitget美股代币现货成交交易对列表，个数
    "okx": [],  # OKX美股代币现货成交交易对列表，个数
}  # 美股代币现货成交交易对映射，映射
XSTOCK_FUTURE_TRADE_SYMBOLS = {
    "bybit": [],  # Bybit美股代币合约成交交易对列表，个数
    "binance": BINANCE_XSTOCK_FUTURE_SYMBOLS,  # Binance美股代币合约成交交易对列表，个数
    "bitget": BITGET_XSTOCK_FUTURE_SYMBOLS,  # Bitget美股代币合约成交交易对列表，个数
    "okx": OKX_XSTOCK_FUTURE_SYMBOLS,  # OKX美股代币合约成交交易对列表，个数
}  # 美股代币合约成交交易对映射，映射
XSTOCK_FUNDING_SYMBOLS = {
    "bybit": [],  # Bybit美股代币资金费交易对列表，个数
    "binance": BINANCE_XSTOCK_FUTURE_SYMBOLS,  # Binance美股代币资金费交易对列表，个数
    "bitget": BITGET_XSTOCK_FUTURE_SYMBOLS,  # Bitget美股代币资金费交易对列表，个数
    "okx": OKX_XSTOCK_FUTURE_SYMBOLS,  # OKX美股代币资金费交易对列表，个数
}  # 美股代币资金费交易对映射，映射

DATASET_SUPPORT = {
    "D10001": {"bybit": False, "binance": False, "bitget": False, "okx": True},  # D10001支持矩阵，映射
    "D10005": {"bybit": False, "binance": False, "bitget": False, "okx": True},  # D10005支持矩阵，映射
    "D10002-4": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10002-4支持矩阵，映射
    "D10006-8": {"bybit": True, "binance": True, "bitget": True, "okx": True},  # D10006-8支持矩阵，映射
    "D10011": {"bybit": False, "binance": False, "bitget": False, "okx": True},  # D10011支持矩阵，映射
    "D10012": {"bybit": False, "binance": False, "bitget": False, "okx": True},  # D10012支持矩阵，映射
    "D10013": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10013支持矩阵，映射
    "D10014": {"bybit": True, "binance": True, "bitget": True, "okx": True},  # D10014支持矩阵，映射
    "D10015": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10015支持矩阵，映射
    "D10016": {"bybit": True, "binance": True, "bitget": True, "okx": True},  # D10016支持矩阵，映射
    "D10017": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10017支持矩阵，映射
    "D10018": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10018支持矩阵，映射
    "D10019": {"bybit": False, "binance": True, "bitget": True, "okx": True},  # D10019支持矩阵，映射
}  # 数据集支持矩阵，映射

DATASET_START_DATES = {
    "D10001": {
        "bybit": {"BTCUSDT": "2025-01-01", "ETHUSDT": "2025-01-01", "SOLUSDT": "2025-01-01"},  # Bybit期货订单簿起始日期映射，映射
        "binance": {"BTCUSDT": "2025-01-01", "ETHUSDT": "2025-01-01", "SOLUSDT": "2025-01-01"},  # Binance期货BBO起始日期映射，映射
        "bitget": {"BTCUSDT": "2025-01-01", "ETHUSDT": "2025-01-01", "SOLUSDT": "2025-01-01"},  # Bitget期货BBO起始日期映射，映射
        "okx": {"BTC-USDT-SWAP": "2025-01-01", "ETH-USDT-SWAP": "2025-01-01", "SOL-USDT-SWAP": "2025-01-01"},  # OKX期货订单簿起始日期映射，映射
    },  # D10001起始日期矩阵，映射
    "D10005": {
        "bybit": {"BTCUSDT": "2025-01-01", "ETHUSDT": "2025-01-01", "SOLUSDT": "2025-01-01"},  # Bybit现货订单簿起始日期映射，映射
        "bitget": {"BTCUSDT": "2025-01-01", "ETHUSDT": "2025-01-01", "SOLUSDT": "2025-01-01"},  # Bitget现货BBO起始日期映射，映射
        "okx": {"BTC-USDT": "2025-01-01", "ETH-USDT": "2025-01-01", "SOL-USDT": "2025-01-01"},  # OKX现货订单簿起始日期映射，映射
    },  # D10005起始日期矩阵，映射
    "D10013": {
        "bybit": {"BTCUSDT": "2020-03-25", "ETHUSDT": "2020-10-21", "SOLUSDT": "2021-06-29"},  # Bybit期货成交起始日期映射，映射
        "binance": {"BTCUSDT": "2019-09-01", "ETHUSDT": "2019-11-01", "SOLUSDT": "2020-09-01"},  # Binance期货成交起始日期映射，映射
        "bitget": {"BTCUSDT": "2024-04-18", "ETHUSDT": "2024-04-18", "SOLUSDT": "2024-04-18"},  # Bitget期货成交起始日期映射，映射
        "okx": {"BTC-USDT-SWAP": "2021-09-01", "ETH-USDT-SWAP": "2021-09-01", "SOL-USDT-SWAP": "2021-09-01"},  # OKX期货成交起始日期映射，映射
    },  # D10013起始日期矩阵，映射
    "D10014": {
        "bybit": {"BTCUSDT": "2022-11-10", "ETHUSDT": "2022-11-10", "SOLUSDT": "2022-11-10"},  # Bybit现货成交起始日期映射，映射
        "binance": {"BTCUSDT": "2017-08-17", "ETHUSDT": "2017-08-17", "SOLUSDT": "2020-08-11"},  # Binance现货成交起始日期映射，映射
        "bitget": {"BTCUSDT": "2024-04-18", "ETHUSDT": "2024-04-18", "SOLUSDT": "2024-04-18"},  # Bitget现货成交起始日期映射，映射
        "okx": {"BTC-USDT": "2021-09-01", "ETH-USDT": "2021-09-01", "SOL-USDT": "2021-09-01"},  # OKX现货成交起始日期映射，映射
    },  # D10014起始日期矩阵，映射
    "D10017": {
        "bybit": {"BTCUSDT": "2019-01-01", "ETHUSDT": "2019-01-01", "SOLUSDT": "2021-01-01"},  # Bybit资金费率起始日期映射，映射
        "binance": {"BTCUSDT": "2020-01-01", "ETHUSDT": "2020-01-01", "SOLUSDT": "2020-09-01"},  # Binance资金费率起始日期映射，映射
        "bitget": {"BTCUSDT": "2021-05-18", "ETHUSDT": "2021-05-18", "SOLUSDT": "2021-07-22"},  # Bitget资金费率起始日期映射，映射
        "okx": {"BTC-USDT-SWAP": "2022-02-01", "ETH-USDT-SWAP": "2022-02-01", "SOL-USDT-SWAP": "2022-02-01"},  # OKX资金费率起始日期映射，映射
    },  # D10017起始日期矩阵，映射
}  # 数据集起始日期矩阵，映射
BYBIT_XSTOCK_SPOT_START_DATES = {  # Bybit美股代币现货成交起始日期映射，映射
    "AAPLXUSDT": "2025-07-01",
    "AMZNXUSDT": "2025-07-07",
    "COINXUSDT": "2025-06-30",
    "CRCLXUSDT": "2025-07-01",
    "GOOGLXUSDT": "2025-07-07",
    "HOODXUSDT": "2025-07-02",
    "MCDXUSDT": "2025-07-08",
    "METAXUSDT": "2025-07-02",
    "NVDAXUSDT": "2025-06-30",
    "TSLAXUSDT": "2025-07-08",
}
BINANCE_XSTOCK_FUTURE_START_DATES = {  # Binance美股代币合约起始日期映射，映射
    "AMZNUSDT": "2026-02-09",
    "COINUSDT": "2026-02-09",
    "CRCLUSDT": "2026-02-09",
    "HOODUSDT": "2026-02-02",
    "INTCUSDT": "2026-02-02",
    "MSTRUSDT": "2026-02-09",
    "PLTRUSDT": "2026-02-09",
    "TSLAUSDT": "2026-01-28",
}
BITGET_XSTOCK_FUTURE_START_DATES = {  # Bitget美股代币合约起始日期映射，映射
    "AAPLUSDT": "2026-02-02",
    "ACNUSDT": "2026-02-02",
    "AMZNUSDT": "2026-02-02",
    "APPUSDT": "2026-02-02",
    "ARMUSDT": "2026-02-02",
    "ASMLUSDT": "2026-02-02",
    "AVGOUSDT": "2026-02-11",
    "BABAUSDT": "2026-02-02",
    "COINUSDT": "2026-02-02",
    "COSTUSDT": "2026-03-02",
    "CRCLUSDT": "2026-02-02",
    "CSCOUSDT": "2026-02-02",
    "FUTUUSDT": "2026-02-02",
    "GEUSDT": "2026-02-02",
    "GMEUSDT": "2026-02-02",
    "GOOGLUSDT": "2026-02-02",
    "HOODUSDT": "2026-02-02",
    "IBMUSDT": "2026-02-02",
    "INTCUSDT": "2026-02-02",
    "JDUSDT": "2026-02-02",
    "LLYUSDT": "2026-02-02",
    "MAUSDT": "2026-02-02",
    "MCDUSDT": "2026-02-02",
    "METAUSDT": "2026-02-02",
    "MRVLUSDT": "2026-02-02",
    "MSFTUSDT": "2026-02-02",
    "MSTRUSDT": "2026-02-02",
    "MUUSDT": "2026-02-11",
    "NVDAUSDT": "2026-02-02",
    "ORCLUSDT": "2026-02-02",
    "PEPUSDT": "2026-02-02",
    "PLTRUSDT": "2026-02-02",
    "QQQUSDT": "2026-02-02",
    "RDDTUSDT": "2026-02-02",
    "SPYUSDT": "2026-02-11",
    "TSLAUSDT": "2026-02-02",
    "TSMUSDT": "2026-03-02",
    "UNHUSDT": "2026-02-02",
    "WMTUSDT": "2026-03-02",
}
OKX_XSTOCK_FUTURE_START_DATES = {  # OKX美股代币合约起始日期映射，映射
    "AAPL-USDT-SWAP": "2026-03-04",
    "AMD-USDT-SWAP": "2026-03-11",
    "AMZN-USDT-SWAP": "2026-02-26",
    "COIN-USDT-SWAP": "2026-02-26",
    "CRCL-USDT-SWAP": "2026-02-26",
    "EWY-USDT-SWAP": "2026-03-11",
    "GOOGL-USDT-SWAP": "2026-03-04",
    "HOOD-USDT-SWAP": "2026-02-25",
    "INTC-USDT-SWAP": "2026-02-26",
    "META-USDT-SWAP": "2026-03-04",
    "MSFT-USDT-SWAP": "2026-03-04",
    "MSTR-USDT-SWAP": "2026-02-25",
    "MU-USDT-SWAP": "2026-03-04",
    "NFLX-USDT-SWAP": "2026-03-11",
    "NVDA-USDT-SWAP": "2026-03-04",
    "ORCL-USDT-SWAP": "2026-03-11",
    "PLTR-USDT-SWAP": "2026-02-26",
    "QQQ-USDT-SWAP": "2026-03-04",
    "SNDK-USDT-SWAP": "2026-03-04",
    "SPY-USDT-SWAP": "2026-03-04",
    "TSLA-USDT-SWAP": "2026-02-25",
}
XSTOCK_DATASET_START_DATES = {
    "D10013": {
        "bybit": {},  # Bybit美股代币合约成交起始日期映射，映射
        "binance": BINANCE_XSTOCK_FUTURE_START_DATES,  # Binance美股代币合约成交起始日期映射，映射
        "bitget": BITGET_XSTOCK_FUTURE_START_DATES,  # Bitget美股代币合约成交起始日期映射，映射
        "okx": OKX_XSTOCK_FUTURE_START_DATES,  # OKX美股代币合约成交起始日期映射，映射
    },  # D10013美股代币起始日期矩阵，映射
    "D10014": {
        "bybit": BYBIT_XSTOCK_SPOT_START_DATES,  # Bybit美股代币现货成交起始日期映射，映射
        "binance": {},  # Binance美股代币现货成交起始日期映射，映射
        "bitget": {},  # Bitget美股代币现货成交起始日期映射，映射
        "okx": {},  # OKX美股代币现货成交起始日期映射，映射
    },  # D10014美股代币起始日期矩阵，映射
    "D10017": {
        "bybit": {},  # Bybit美股代币资金费起始日期映射，映射
        "binance": BINANCE_XSTOCK_FUTURE_START_DATES,  # Binance美股代币资金费起始日期映射，映射
        "bitget": BITGET_XSTOCK_FUTURE_START_DATES,  # Bitget美股代币资金费起始日期映射，映射
        "okx": OKX_XSTOCK_FUTURE_START_DATES,  # OKX美股代币资金费起始日期映射，映射
    },  # D10017美股代币起始日期矩阵，映射
}  # 美股代币起始日期矩阵，映射

INSURANCE_SYMBOLS = {
    "bybit": ["USDT"],  # Bybit保险基金币种列表，个数
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],  # Binance保险基金交易对列表，个数
    "bitget": ["BTC"],  # Bitget保险基金币种列表，个数
    "okx": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],  # OKX保险基金家族列表，个数
}  # 保险基金标的映射，映射
EARN_COINS = {
    "bybit": ["BTC", "ETH", "SOL"],  # Bybit理财币种列表，个数
    "binance": ["BTC", "ETH", "SOL"],  # Binance理财币种列表，个数
    "bitget": ["BTC", "ETH", "SOL"],  # Bitget理财币种列表，个数
    "okx": ["BTC", "ETH", "SOL"],  # OKX链上币种列表，个数
}  # 理财币种映射，映射
DATA_DYLAN_ROOT = Path("data")  # 数据根目录，路径


def build_data_dir(prefix: str, dataset_name: str) -> Path:
    """构造数据目录。"""
    return DATA_DYLAN_ROOT / prefix / dataset_name


def build_standard_source_dir(dataset_id: str, exchange: str) -> Path | None:
    """按统一命名构造源目录。"""
    if dataset_id == "D10001":
        return build_data_dir("src", f"{exchange}_future_orderbook_di")
    if dataset_id == "D10005":
        return build_data_dir("src", f"{exchange}_spot_orderbook_di")
    if dataset_id == "D10013":
        return build_data_dir("src", f"{exchange}_future_trade_di")
    if dataset_id == "D10014":
        return build_data_dir("src", f"{exchange}_spot_trade_di")
    if dataset_id == "D10017":
        return build_data_dir("src", f"{exchange}_future_fundingrate_di")
    if dataset_id == "D10018":
        return build_data_dir("src", f"{exchange}_insurance_di")
    if dataset_id == "D10019":
        return build_data_dir("src", f"{exchange}_onchainstaking_di")
    return None


def build_standard_output_dir(dataset_id: str, exchange: str) -> Path | None:
    """按统一命名构造输出目录。"""
    if dataset_id == "D10011":
        return build_data_dir("dwd", f"dwd_{exchange}_future_ob_ss_di")
    if dataset_id == "D10012":
        return build_data_dir("dwd", f"dwd_{exchange}_spot_ob_ss_di")
    if dataset_id == "D10015":
        return build_data_dir("dws", f"dws_{exchange}_future_trade_1s_di")
    if dataset_id == "D10016":
        return build_data_dir("dws", f"dws_{exchange}_spot_trade_1s_di")
    return None


def build_standard_orderbook_rt_dir(exchange: str, market: str, stage: str) -> Path:
    """按统一命名构造实时订单簿目录。"""
    return build_data_dir("src", f"{exchange}_{market}_orderbook_{stage}")


def build_standard_orderbook_rt_tag(exchange: str, market: str, stage: str) -> str:
    """按统一命名构造实时订单簿标签。"""
    return f"{exchange}_{market}_orderbook_{stage}"


DATASET_SOURCE_DIRS = {
    "D10001": {
        "bybit": build_standard_source_dir("D10001", "bybit"),  # Bybit期货订单簿源目录，路径
        "binance": build_standard_source_dir("D10001", "binance"),  # Binance期货订单簿源目录，路径
        "bitget": build_standard_source_dir("D10001", "bitget"),  # Bitget期货订单簿源目录，路径
        "okx": build_standard_source_dir("D10001", "okx"),  # OKX期货订单簿源目录，路径
    },  # D10001源目录映射，映射
    "D10005": {
        "bybit": build_standard_source_dir("D10005", "bybit"),  # Bybit现货订单簿源目录，路径
        "bitget": build_standard_source_dir("D10005", "bitget"),  # Bitget现货订单簿源目录，路径
        "okx": build_standard_source_dir("D10005", "okx"),  # OKX现货订单簿源目录，路径
    },  # D10005源目录映射，映射
    "D10013": {
        "bybit": build_standard_source_dir("D10013", "bybit"),  # Bybit期货成交源目录，路径
        "binance": build_standard_source_dir("D10013", "binance"),  # Binance期货成交源目录，路径
        "bitget": build_standard_source_dir("D10013", "bitget"),  # Bitget期货成交源目录，路径
        "okx": build_standard_source_dir("D10013", "okx"),  # OKX期货成交源目录，路径
    },  # D10013源目录映射，映射
    "D10014": {
        "bybit": build_standard_source_dir("D10014", "bybit"),  # Bybit现货成交源目录，路径
        "binance": build_standard_source_dir("D10014", "binance"),  # Binance现货成交源目录，路径
        "bitget": build_standard_source_dir("D10014", "bitget"),  # Bitget现货成交源目录，路径
        "okx": build_standard_source_dir("D10014", "okx"),  # OKX现货成交源目录，路径
    },  # D10014源目录映射，映射
    "D10017": {
        "bybit": build_standard_source_dir("D10017", "bybit"),  # Bybit资金费率源目录，路径
        "binance": build_standard_source_dir("D10017", "binance"),  # Binance资金费率源目录，路径
        "bitget": build_standard_source_dir("D10017", "bitget"),  # Bitget资金费率源目录，路径
        "okx": build_standard_source_dir("D10017", "okx"),  # OKX资金费率源目录，路径
    },  # D10017源目录映射，映射
    "D10018": {
        "bybit": build_standard_source_dir("D10018", "bybit"),  # Bybit保险基金源目录，路径
        "binance": build_standard_source_dir("D10018", "binance"),  # Binance保险基金源目录，路径
        "bitget": build_standard_source_dir("D10018", "bitget"),  # Bitget保险基金源目录，路径
        "okx": build_standard_source_dir("D10018", "okx"),  # OKX保险基金源目录，路径
    },  # D10018源目录映射，映射
    "D10019": {
        "bybit": build_standard_source_dir("D10019", "bybit"),  # Bybit链上源目录，路径
        "binance": build_standard_source_dir("D10019", "binance"),  # Binance链上源目录，路径
        "bitget": build_standard_source_dir("D10019", "bitget"),  # Bitget链上源目录，路径
        "okx": build_standard_source_dir("D10019", "okx"),  # OKX链上源目录，路径
    },  # D10019源目录映射，映射
}  # 源目录矩阵，映射
DATASET_OUTPUT_DIRS = {
    "D10011": {
        "bybit": build_standard_output_dir("D10011", "bybit"),  # Bybit期货订单簿快照输出目录，路径
        "binance": build_standard_output_dir("D10011", "binance"),  # Binance期货订单簿快照输出目录，路径
        "bitget": build_standard_output_dir("D10011", "bitget"),  # Bitget期货订单簿快照输出目录，路径
        "okx": build_standard_output_dir("D10011", "okx"),  # OKX期货订单簿快照输出目录，路径
    },  # D10011输出目录映射，映射
    "D10012": {
        "bybit": build_standard_output_dir("D10012", "bybit"),  # Bybit现货订单簿快照输出目录，路径
        "bitget": build_standard_output_dir("D10012", "bitget"),  # Bitget现货订单簿快照输出目录，路径
        "okx": build_standard_output_dir("D10012", "okx"),  # OKX现货订单簿快照输出目录，路径
    },  # D10012输出目录映射，映射
    "D10015": {
        "bybit": build_standard_output_dir("D10015", "bybit"),  # Bybit期货成交输出目录，路径
        "binance": build_standard_output_dir("D10015", "binance"),  # Binance期货成交输出目录，路径
        "bitget": build_standard_output_dir("D10015", "bitget"),  # Bitget期货成交输出目录，路径
        "okx": build_standard_output_dir("D10015", "okx"),  # OKX期货成交输出目录，路径
    },  # D10015输出目录映射，映射
    "D10016": {
        "bybit": build_standard_output_dir("D10016", "bybit"),  # Bybit现货成交输出目录，路径
        "binance": build_standard_output_dir("D10016", "binance"),  # Binance现货成交输出目录，路径
        "bitget": build_standard_output_dir("D10016", "bitget"),  # Bitget现货成交输出目录，路径
        "okx": build_standard_output_dir("D10016", "okx"),  # OKX现货成交输出目录，路径
    },  # D10016输出目录映射，映射
}  # 输出目录矩阵，映射

UNSUPPORTED_STATUS_TEXT = "未支持"  # 未支持状态文本，字符串
PAUSE_REQUESTED_STATUS_TEXT = "暂停中"  # 暂停请求状态文本，字符串
PAUSED_STATUS_TEXT = "已暂停"  # 已暂停状态文本，字符串
PAUSED_DATASET_EXCHANGES = {}  # 数据集与交易所暂停状态映射，映射
PAUSE_REQUESTED_DATASET_EXCHANGES = {}  # 数据集与交易所暂停请求映射，映射
TASK_CONTROL_LOCK = threading.Lock()  # 任务控制状态锁，锁
TASK_CONTROL_EVENT = threading.Event()  # 任务控制唤醒事件，事件
RUNTIME_MEMORY_METRICS = {}  # 运行时内存观测映射，映射
RUNTIME_MEMORY_METRICS_LOCK = threading.Lock()  # 运行时内存观测锁，锁
RUNTIME_TARGET_MODE = ""  # 运行时专项模式标识，字符串
OKX_ETH_PRIORITY_TASK_IDS = {"D10001", "D10005", "D10011", "D10012", "D10013", "D10014"}  # OKX专项任务集合，个数
FUTURE_RAW_PRIORITY_TASK_IDS = {"D10001", "D10013"}  # 原始期货专项任务集合，个数
RUNTIME_TARGET_SCOPE = {"exchanges": [], "base_coin": "", "start_date": "", "end_date": ""}  # 运行时专项范围，映射


def set_runtime_target_mode(mode: str) -> None:
    """设置运行时专项模式。"""
    global RUNTIME_TARGET_MODE
    RUNTIME_TARGET_MODE = mode


def set_runtime_target_scope(exchanges: list[str], base_coin: str, start_date: str, end_date: str) -> None:
    """设置运行时专项范围。"""
    RUNTIME_TARGET_SCOPE["exchanges"] = list(exchanges)
    RUNTIME_TARGET_SCOPE["base_coin"] = str(base_coin)
    RUNTIME_TARGET_SCOPE["start_date"] = str(start_date)
    RUNTIME_TARGET_SCOPE["end_date"] = str(end_date)


def is_okx_eth_priority_mode() -> bool:
    """判断是否启用OKX的ETH专项模式。"""
    return RUNTIME_TARGET_MODE == "okx_eth_priority"


def is_future_raw_priority_mode() -> bool:
    """判断是否启用原始期货专项模式。"""
    return RUNTIME_TARGET_MODE == "future_raw_priority"


def get_okx_eth_priority_task_ids() -> list[str]:
    """返回OKX的ETH专项任务列表。"""
    return sorted(OKX_ETH_PRIORITY_TASK_IDS)


def get_future_raw_priority_task_ids() -> list[str]:
    """返回原始期货专项任务列表。"""
    return sorted(FUTURE_RAW_PRIORITY_TASK_IDS)


def get_runtime_target_exchanges() -> list[str]:
    """返回运行时专项交易所列表。"""
    return list(RUNTIME_TARGET_SCOPE["exchanges"])


def get_runtime_target_base_coin() -> str:
    """返回运行时专项基础币种。"""
    return str(RUNTIME_TARGET_SCOPE["base_coin"])


def get_runtime_target_start_date() -> str:
    """返回运行时专项起始日期。"""
    return str(RUNTIME_TARGET_SCOPE["start_date"])


def get_runtime_target_end_date() -> str:
    """返回运行时专项结束日期。"""
    return str(RUNTIME_TARGET_SCOPE["end_date"])


def get_runtime_future_symbol(exchange: str) -> str:
    """返回运行时专项永续交易对。"""
    base_coin = get_runtime_target_base_coin()
    if not base_coin:
        return ""
    if exchange == "okx":
        return f"{base_coin}-USDT-SWAP"
    return f"{base_coin}USDT"


def filter_runtime_symbols(exchange: str, symbols: list[str]) -> list[str]:
    """按运行时专项模式过滤交易对列表。"""
    if is_future_raw_priority_mode():
        target_symbol = get_runtime_future_symbol(exchange)
        if not target_symbol:
            return []
        return [symbol for symbol in symbols if symbol == target_symbol]
    if not is_okx_eth_priority_mode() or exchange != "okx":
        return list(symbols)
    allowed = {"ETH-USDT", "ETH-USDT-SWAP"}  # OKX专项允许交易对集合，个数
    return [symbol for symbol in symbols if symbol in allowed]


def list_exchanges() -> list:
    """返回全部CEX交易所。"""
    if is_future_raw_priority_mode():
        return [exchange for exchange in get_runtime_target_exchanges() if EXCHANGE_ENABLED.get(exchange, False)]
    if is_okx_eth_priority_mode():
        return ["okx"] if EXCHANGE_ENABLED.get("okx", False) else []
    return [exchange for exchange in CEX_EXCHANGES if EXCHANGE_ENABLED.get(exchange, False)]


def is_exchange_enabled(exchange: str) -> bool:
    """判断交易所总开关是否开启。"""
    return bool(EXCHANGE_ENABLED.get(exchange, False))


def merge_symbols(base_symbols: list, extra_symbols: list) -> list:
    """合并交易对列表并去重排序。"""
    merged = []
    seen = set()
    for symbol in base_symbols:
        if symbol in seen:
            continue
        merged.append(symbol)
        seen.add(symbol)
    for symbol in sorted(extra_symbols):
        if symbol in seen:
            continue
        merged.append(symbol)
        seen.add(symbol)
    return merged


def get_spot_symbols(exchange: str) -> list:
    """返回指定交易所现货交易对。"""
    if is_future_raw_priority_mode():
        return []
    if is_okx_eth_priority_mode() and exchange == "okx":
        return ["ETH-USDT"]
    return list(SPOT_SYMBOLS.get(exchange, []))


def get_future_symbols(exchange: str) -> list:
    """返回指定交易所永续交易对。"""
    if is_future_raw_priority_mode():
        return filter_runtime_symbols(exchange, list(FUTURE_PERPETUAL_SYMBOLS.get(exchange, [])))
    if is_okx_eth_priority_mode() and exchange == "okx":
        return ["ETH-USDT-SWAP"]
    return list(FUTURE_PERPETUAL_SYMBOLS.get(exchange, []))


def get_spot_trade_symbols(exchange: str) -> list:
    """返回指定交易所现货成交交易对。"""
    if is_future_raw_priority_mode():
        return []
    if is_okx_eth_priority_mode() and exchange == "okx":
        return ["ETH-USDT"]
    return merge_symbols(SPOT_SYMBOLS.get(exchange, []), XSTOCK_SPOT_TRADE_SYMBOLS.get(exchange, []))


def get_future_trade_symbols(exchange: str) -> list:
    """返回指定交易所期货成交交易对。"""
    if is_future_raw_priority_mode():
        return filter_runtime_symbols(exchange, merge_symbols(FUTURE_PERPETUAL_SYMBOLS.get(exchange, []), XSTOCK_FUTURE_TRADE_SYMBOLS.get(exchange, [])))
    if is_okx_eth_priority_mode() and exchange == "okx":
        return ["ETH-USDT-SWAP"]
    return merge_symbols(FUTURE_PERPETUAL_SYMBOLS.get(exchange, []), XSTOCK_FUTURE_TRADE_SYMBOLS.get(exchange, []))


def get_funding_symbols(exchange: str) -> list:
    """返回指定交易所资金费交易对。"""
    return merge_symbols(FUTURE_PERPETUAL_SYMBOLS.get(exchange, []), XSTOCK_FUNDING_SYMBOLS.get(exchange, []))


def get_delivery_families(exchange: str) -> list:
    """返回指定交易所交割合约家族。"""
    if is_future_raw_priority_mode():
        return []
    if is_okx_eth_priority_mode() and exchange == "okx":
        return ["ETH-USDT"]
    return list(FUTURE_DELIVERY_FAMILIES.get(exchange, []))


def get_supported_exchanges(dataset_id: str) -> list:
    """返回指定数据集支持的交易所。"""
    return [exchange for exchange in list_exchanges() if is_supported(dataset_id, exchange)]


def is_supported(dataset_id: str, exchange: str) -> bool:
    """判断指定数据集是否支持该交易所。"""
    if is_future_raw_priority_mode():
        if exchange not in get_runtime_target_exchanges():
            return False
        if dataset_id not in FUTURE_RAW_PRIORITY_TASK_IDS:
            return False
        return is_exchange_enabled(exchange)
    if is_okx_eth_priority_mode():
        if exchange != "okx":
            return False
        if dataset_id not in OKX_ETH_PRIORITY_TASK_IDS:
            return False
    return is_exchange_enabled(exchange) and bool(DATASET_SUPPORT.get(dataset_id, {}).get(exchange))


def is_paused(dataset_id: str, exchange: str) -> bool:
    """判断指定数据集是否已暂停。"""
    with TASK_CONTROL_LOCK:
        return bool(PAUSED_DATASET_EXCHANGES.get((dataset_id, exchange), False))


def is_pause_requested(dataset_id: str, exchange: str) -> bool:
    """判断指定数据集是否已请求暂停。"""
    with TASK_CONTROL_LOCK:
        return bool(PAUSE_REQUESTED_DATASET_EXCHANGES.get((dataset_id, exchange), False))


def is_pause_pending(dataset_id: str, exchange: str) -> bool:
    """判断指定数据集是否处于暂停流程。"""
    with TASK_CONTROL_LOCK:
        return bool(
            PAUSED_DATASET_EXCHANGES.get((dataset_id, exchange), False)
            or PAUSE_REQUESTED_DATASET_EXCHANGES.get((dataset_id, exchange), False)
        )


def set_paused(dataset_id: str, exchange: str, paused: bool) -> bool:
    """设置指定数据集的已暂停状态。"""
    with TASK_CONTROL_LOCK:
        if paused:
            PAUSED_DATASET_EXCHANGES[(dataset_id, exchange)] = True
            PAUSE_REQUESTED_DATASET_EXCHANGES.pop((dataset_id, exchange), None)
        else:
            PAUSED_DATASET_EXCHANGES.pop((dataset_id, exchange), None)
            PAUSE_REQUESTED_DATASET_EXCHANGES.pop((dataset_id, exchange), None)
    TASK_CONTROL_EVENT.set()
    return paused


def request_pause(dataset_id: str, exchange: str) -> str:
    """请求指定数据集在安全边界暂停。"""
    with TASK_CONTROL_LOCK:
        key = (dataset_id, exchange)
        if PAUSED_DATASET_EXCHANGES.get(key):
            PAUSED_DATASET_EXCHANGES.pop(key, None)
            PAUSE_REQUESTED_DATASET_EXCHANGES.pop(key, None)
            state = "running"
        elif PAUSE_REQUESTED_DATASET_EXCHANGES.get(key):
            PAUSE_REQUESTED_DATASET_EXCHANGES.pop(key, None)
            state = "running"
        else:
            PAUSE_REQUESTED_DATASET_EXCHANGES[key] = True
            state = "pause_requested"
    TASK_CONTROL_EVENT.set()
    return state


def toggle_paused(dataset_id: str, exchange: str) -> str:
    """切换指定数据集的暂停状态。"""
    return request_pause(dataset_id, exchange)


def apply_pause_if_requested(dataset_id: str, exchange: str) -> bool:
    """在安全边界将暂停请求落为已暂停。"""
    with TASK_CONTROL_LOCK:
        key = (dataset_id, exchange)
        if PAUSE_REQUESTED_DATASET_EXCHANGES.get(key):
            PAUSE_REQUESTED_DATASET_EXCHANGES.pop(key, None)
            PAUSED_DATASET_EXCHANGES[key] = True
            changed = True
        else:
            changed = False
        paused = bool(PAUSED_DATASET_EXCHANGES.get(key))
    if changed:
        TASK_CONTROL_EVENT.set()
    return paused


def wait_with_task_control(seconds: int | float) -> None:
    """按秒等待并响应任务控制变更。"""
    deadline = time.time() + max(0.0, float(seconds))
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            return
        triggered = TASK_CONTROL_EVENT.wait(timeout=min(1.0, remaining))
        if triggered:
            TASK_CONTROL_EVENT.clear()
            return


def update_runtime_memory_metrics(dataset_id: str, exchange: str, metrics: dict) -> None:
    """更新指定数据集的运行时内存观测。"""
    with RUNTIME_MEMORY_METRICS_LOCK:
        key = (dataset_id, exchange)
        bucket = RUNTIME_MEMORY_METRICS.setdefault(key, {})
        bucket.update(metrics)


def clear_runtime_memory_metrics(dataset_id: str, exchange: str) -> None:
    """清理指定数据集的运行时内存观测。"""
    with RUNTIME_MEMORY_METRICS_LOCK:
        RUNTIME_MEMORY_METRICS.pop((dataset_id, exchange), None)


def get_runtime_memory_metrics(dataset_id: str, exchange: str) -> dict:
    """返回指定数据集的运行时内存观测。"""
    with RUNTIME_MEMORY_METRICS_LOCK:
        return dict(RUNTIME_MEMORY_METRICS.get((dataset_id, exchange), {}))


def get_start_date(dataset_id: str, exchange: str, symbol: str) -> str:
    """返回指定数据集的起始日期。"""
    if is_future_raw_priority_mode() and exchange in get_runtime_target_exchanges() and dataset_id in FUTURE_RAW_PRIORITY_TASK_IDS:
        return get_runtime_target_start_date()
    dataset_map = DATASET_START_DATES.get(dataset_id, {})
    exchange_map = dataset_map.get(exchange, {})
    if symbol in exchange_map:
        return exchange_map[symbol]
    xstock_dataset_map = XSTOCK_DATASET_START_DATES.get(dataset_id, {})
    return xstock_dataset_map.get(exchange, {}).get(symbol, "")


def get_min_start_date(dataset_id: str, exchange: str) -> str:
    """返回指定数据集的最小起始日期。"""
    if is_future_raw_priority_mode() and exchange in get_runtime_target_exchanges() and dataset_id in FUTURE_RAW_PRIORITY_TASK_IDS:
        return get_runtime_target_start_date()
    base_values = list(DATASET_START_DATES.get(dataset_id, {}).get(exchange, {}).values())
    xstock_values = list(XSTOCK_DATASET_START_DATES.get(dataset_id, {}).get(exchange, {}).values())
    values = base_values + xstock_values
    if not values:
        return ""
    return min(values)


def get_max_end_date(dataset_id: str, exchange: str) -> str:
    """返回指定数据集的结束日期上限。"""
    if is_future_raw_priority_mode() and exchange in get_runtime_target_exchanges() and dataset_id in FUTURE_RAW_PRIORITY_TASK_IDS:
        return get_runtime_target_end_date()
    return ""


def get_source_dir(dataset_id: str, exchange: str) -> Path | None:
    """返回指定数据集的源目录。"""
    return DATASET_SOURCE_DIRS.get(dataset_id, {}).get(exchange)


def get_output_dir(dataset_id: str, exchange: str) -> Path | None:
    """返回指定数据集的输出目录。"""
    return DATASET_OUTPUT_DIRS.get(dataset_id, {}).get(exchange)


def get_insurance_symbols(exchange: str) -> list:
    """返回指定交易所保险基金标的。"""
    return list(INSURANCE_SYMBOLS.get(exchange, []))


def get_earn_coins(exchange: str) -> list:
    """返回指定交易所理财币种。"""
    return list(EARN_COINS.get(exchange, []))


def get_status_key(exchange: str, market: str, symbol: str) -> str:
    """返回统一状态键。"""
    return f"{exchange}/{market}/{symbol}"
