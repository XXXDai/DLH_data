from pathlib import Path
import sys


DATA_DIR = Path("data")  # 数据根目录，路径


def iter_delete_targets(root: Path) -> list[Path]:
    targets = []
    if not root.exists():
        return targets
    for path in root.rglob("*"):
        if path.is_dir():
            continue
        targets.append(path)
    targets.sort(key=lambda p: str(p))
    return targets


def delete_files_keep_dirs(root: Path) -> int:
    targets = iter_delete_targets(root)
    for path in targets:
        path.unlink()
    return len(targets)


def main() -> None:
    if not DATA_DIR.exists():
        print(f"目录不存在: {DATA_DIR}")
        return
    if not DATA_DIR.is_dir():
        raise ValueError(f"路径不是目录: {DATA_DIR}")
    targets = iter_delete_targets(DATA_DIR)
    if not targets:
        print("未找到可删除的数据文件")
        return
    print("将删除 data/ 下的所有文件，但保留目录结构")
    print(f"待删除文件数: {len(targets)}")
    confirm = input("确认请输入 YES（大小写不敏感）: ").strip().lower()
    if confirm != "yes":
        print("已取消")
        return
    deleted = delete_files_keep_dirs(DATA_DIR)
    print(f"已删除文件数: {deleted}")


if __name__ == "__main__":
    sys.exit(main())
