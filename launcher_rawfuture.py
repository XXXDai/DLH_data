import sys

import launcher


DEFAULT_RAW_FUTURE_SCOPE = "-rawfuture=okx,binance:ETH:2026-02-01:2026-03-31"  # 默认原始期货专项范围，字符串


def prepare_runtime_args() -> None:
    """补齐独立启动器需要的默认参数。"""
    if "-s3" not in sys.argv:
        sys.argv.append("-s3")
    if "-nowss" not in sys.argv:
        sys.argv.append("-nowss")
    if not any(arg.startswith("-rawfuture=") for arg in sys.argv):
        sys.argv.append(DEFAULT_RAW_FUTURE_SCOPE)


def list_no_wait_launcher_processes() -> list[str]:
    """独立启动器不等待旧进程切换。"""
    return []


def main() -> None:
    """运行原始期货专项独立启动器。"""
    prepare_runtime_args()
    launcher.apply_storage_mode_from_argv()
    launcher.cex_common.set_upload_startup_sync_enabled(False)
    launcher.cex_common.set_storage_s3_read_enabled(False)
    launcher.list_other_launcher_processes = list_no_wait_launcher_processes
    launcher.main()


if __name__ == "__main__":
    main()
