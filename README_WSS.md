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

当前这些配置都从这个文件读取：

- `s3.bucket_name`
- `s3.prefix`
- `write.file_segment_seconds`
- `write.buffer_lines`
- `write.buffer_bytes`
- `write.buffer_interval_seconds`
- `exchange_enabled`
- `spot_symbols`
- `future_perpetual_symbols`
- `future_delivery_families`
- `dataset_support`

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

当前收集的交易对：

- 现货 `binance`：`BTCUSDT`、`ETHUSDT`、`SOLUSDT`
- 现货 `bitget`：`BTCUSDT`、`ETHUSDT`、`SOLUSDT`
- 现货 `okx`：`BTC-USDT`、`ETH-USDT`、`SOL-USDT`
- 期货 `binance`：`BTCUSDT`、`ETHUSDT`、`SOLUSDT`
- 期货 `bitget`：`BTCUSDT`、`ETHUSDT`、`SOLUSDT`
- 期货 `okx` 永续：`BTC-USDT-SWAP`、`ETH-USDT-SWAP`、`SOL-USDT-SWAP`
- 期货 `okx` 交割：按 `BTC-USDT`、`ETH-USDT`、`SOL-USDT` 动态刷新当前可交易合约

## 修改位置

- 增加或修改交易所启用状态：`launcher_wss_config.json` 里的 `exchange_enabled`
- 增加或修改现货代币对：`launcher_wss_config.json` 里的 `spot_symbols`
- 增加或修改期货永续代币对：`launcher_wss_config.json` 里的 `future_perpetual_symbols`
- 增加或修改 `okx` 交割合约家族：`launcher_wss_config.json` 里的 `future_delivery_families`
- 修改哪些交易所支持哪些数据集：`launcher_wss_config.json` 里的 `dataset_support`
- 修改实际上传到 `S3` 的间隔：`launcher_wss_config.json` 里的 `write.file_segment_seconds`
- 当前默认是 `3600` 秒，也就是按小时切文件并在切文件时上传
- 如果改成 `60`，就会按分钟切文件并上传，但目录仍然按小时聚合，不会再增加更细的目录层级

## 落盘逻辑

- 数据先写入内存缓冲，不是每条消息都立刻刷到磁盘
- 当前刷盘阈值在 `launcher_wss_config.json` 的 `write` 下面
- `buffer_lines`：缓冲行数达到阈值后刷盘
- `buffer_bytes`：缓冲字节数达到阈值后刷盘
- `buffer_interval_seconds`：距离上次刷盘超过这个秒数后刷盘
- 文件按 `write.file_segment_seconds` 切分，切到新文件时会先关闭旧文件，关闭时会先刷盘，再进入上传队列
- 目录始终按小时聚合；如果切分粒度改成分钟或秒，只会让文件名变细，不会继续新增目录层级


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
