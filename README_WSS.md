# launcher_wss.py

## 启动参数

```bash
python3 launcher_wss.py
python3 launcher_wss.py -s3
python3 launcher_wss.py -rm
python3 launcher_wss.py -s3 -rm
```

- 不带参数：本地模式，只写本地
- `-s3`：开启上传
- `-rm`：启动前清空本地数据目录

## 环境变量

不设置时默认：

```bash
DLH_S3_BUCKET_NAME=main-ai-ext
DLH_S3_PREFIX=dlh/data/dylan
```

覆盖示例：

```bash
export DLH_S3_BUCKET_NAME=main-ai-ext
export DLH_S3_PREFIX=dlh/data/dylan
python3 launcher_wss.py -s3
```

## 日志样式

终端直接输出运行日志。

常见格式：

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
