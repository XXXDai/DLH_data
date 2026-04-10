# launcher_wss.py

## 启动参数

```bash
python3 launcher_wss.py
python3 launcher_wss.py -s3
python3 launcher_wss.py -rm
python3 launcher_wss.py -s3 -rm
```

```bash
nohup python3 launcher_wss.py -s3 > logs/launcher_wss.log 2>&1 &
```

- 不带参数：本地模式，只写本地
- `-s3`：开启上传
- `-rm`：启动前清空本地数据目录

## 配置文件

配置文件固定为：

```text
launcher_wss_config.json
```

配置结构和含义一一对应如下：

```json
{
  "s3": {
    "bucket_name": "main-ai-ext",
    "prefix": "dlh/data/dylan"
  },
  "write": {
    "file_segment_seconds": 3600,
    "buffer_lines": 2048,
    "buffer_bytes": 8388608,
    "buffer_interval_seconds": 15
  },
  "exchange_enabled": {
    "bybit": false,
    "binance": true,
    "bitget": true,
    "okx": true
  },
  "spot_symbols": {
    "bybit": [],
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "bitget": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okx": ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
  },
  "future_perpetual_symbols": {
    "bybit": [],
    "binance": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "bitget": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
    "okx": ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP"]
  },
  "future_delivery_families": {
    "bybit": [],
    "binance": [],
    "bitget": [],
    "okx": ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
  },
  "dataset_support": {
    "D10002-4": {
      "bybit": false,
      "binance": true,
      "bitget": true,
      "okx": true
    },
    "D10006-8": {
      "bybit": false,
      "binance": true,
      "bitget": true,
      "okx": true
    }
  }
}
```

- `s3.bucket_name`：S3 bucket 名称
- `s3.prefix`：S3 根目录前缀
- `write.file_segment_seconds`：切文件并触发上传的时间间隔，单位秒
- `write.buffer_lines`：单文件缓冲达到多少行后刷盘
- `write.buffer_bytes`：单文件缓冲达到多少字节后刷盘
- `write.buffer_interval_seconds`：单文件最长多久强制刷盘一次，单位秒
- `exchange_enabled.<交易所>`：是否启用该交易所
- `spot_symbols.<交易所>`：该交易所现货要收集的交易对
- `future_perpetual_symbols.<交易所>`：该交易所永续要收集的交易对
- `future_delivery_families.okx`：OKX 交割合约家族，程序会动态刷新当前可交易合约
- `dataset_support.D10002-4.<交易所>`：该交易所是否启用期货实时订单簿
- `dataset_support.D10006-8.<交易所>`：该交易所是否启用现货实时订单簿

- 修改 `launcher_wss_config.json` 后不需要重启，程序会自动检测，并在下一个整点切到新

配置

## 当前收集的数据集

- `D10002-4`：期货实时订单簿
- `D10006-8`：现货实时订单簿

当前启用交易所：

- `binance`
- `bitget`
- `okx`

每个交易对都会产出三层数据：

- `orderbook_rt`
- `orderbook_rt_ss`
- `orderbook_rt_ss_1s`


查看日志：

```bash
tail -f logs/launcher_wss.log
```

常见输出：

```text
[D10002-4] okx future BTC-USDT-SWAP 主连接异常，准备重连: Connection to remote host was lost.
[D10006-8] binance spot BTCUSDT 已写入: data/src/binance_spot_orderbook_rt/BTCUSDT/2026041012/BTCUSDT-binance_spot_orderbook_rt-2026041012.json
==== DLH WSS 状态摘要 | 存储: s3 | 2026-04-10 12:00:00 ====
内存: 300 MB | 峰值: 420 MB | WS缓存 future 0文件/0行 | spot 0文件/0行
```

本地只保留错误日志：

```text
logs/error.log
```
