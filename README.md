# 项目说明

本项目用于自动维护多类交易所数据的下载与处理，提供统一的 TUI 启动管理与运行状态展示，支持按频率自动触发任务并持续补数。
当前采集范围以四家中心化交易所为主：`Bybit / Binance / Bitget / OKX`，主币种保留 `BTC / ETH / SOL`，并额外覆盖可直接下载的 xstock 美股代币成交与资金费。
`Polymarket` 相关代码仍保留在仓库中，但当前不会被 `launcher.py` 启动。

## 功能概览
- 统一入口 `launcher.py`，集中管理 `D10001-D10019` 的下载、处理与 WS 任务
- 任务按频率自动运行（每日、每小时、实时等）
- 失败记录与重试机制
- TUI 实时展示运行状态与日志
- 历史成交下载优先走月度归档，拿不到时自动回退到日度归档

## 运行前准备
- Python 运行环境需可执行 `launcher.py`
- 依赖安装：`pip install -r requirements.txt`

## 运行方式
使用 `tmux` 后台运行，断开终端不影响进程：
1. 启动会话：`tmux new -s dlh`
2. 运行主程序：`python3 launcher.py`
3. 脱离会话但不停止进程：按 `Ctrl-b` 再按 `d`
4. 重新查看界面：`tmux attach -t dlh`
5. 关闭会话：`tmux kill-session -t dlh`

## 配置说明
公共运行参数在 `app_config.py`，包括超时、重试、TUI 与调度参数等。
中心化交易所配置在 `cex/cex_config.py`，包括四家交易所支持矩阵、主币种交易对、xstock 交易对和各数据集起始日期。
Polymarket 专用配置在 `poly_config.py`，当前仅供保留代码引用，不参与主启动流程。

## 常用操作
- 清理数据但保留目录：`/Users/xdai/miniconda3/bin/python /Users/xdai/Documents/projects/Week1/smi/clear_data.py`
- 校验已下载数据是否符合配置：`python3 validate_data.py`
