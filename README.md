# 项目说明

本项目用于自动维护多类交易所数据的下载与处理，提供统一的 TUI 启动管理与运行状态展示，支持按频率自动触发任务并持续补数。

## 功能概览
- 统一入口 `launcher.py`，集中管理下载、处理与 WS 任务
- 任务按频率自动运行（每日、每小时、实时等）
- 失败记录与重试机制
- TUI 实时展示运行状态与日志

## 运行前准备
- Python 运行环境需可执行 `launcher.py`
- 依赖安装：`pip install -r requirements.txt`

## 运行方式
使用 `tmux` 后台运行，断开终端不影响进程：
1. 启动会话：`tmux new -s smi`
2. 运行主程序：`python3 launcher.py`
3. 脱离会话但不停止进程：按 `Ctrl-b` 再按 `d`
4. 重新查看界面：`tmux attach -t smi`
5. 关闭会话：`tmux kill-session -t smi`

## 配置说明
核心配置在 `app_config.py`，包括交易对、起始日期、日志与 TUI 参数等。

## 常用操作
- 清理数据但保留目录：`/Users/xdai/miniconda3/bin/python /Users/xdai/Documents/projects/Week1/smi/clear_data.py`
- 校验已下载数据是否符合配置：`python3 validate_data.py`
