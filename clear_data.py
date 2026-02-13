from pathlib import Path
import shutil
import sys


DATA_DIR = Path("data")  # 数据根目录，路径
KEEP_SUBDIRS = {"src", "dwd", "dws"}  # 保留一级目录集合，个数


def count_tree(root: Path) -> tuple[int, int]:
    file_count = 0
    dir_count = 0
    if not root.exists():
        return 0, 0
    for path in root.rglob("*"):
        if path.is_dir():
            dir_count += 1
        else:
            file_count += 1
    return file_count, dir_count


def compute_delete_counts(root: Path) -> tuple[int, int]:
    file_count = 0
    dir_count = 0
    for child in root.iterdir():
        if child.is_dir():
            if child.name in KEEP_SUBDIRS:
                child_files, child_dirs = count_tree(child)
                file_count += child_files
                dir_count += child_dirs
                continue
            child_files, child_dirs = count_tree(child)
            file_count += child_files
            dir_count += child_dirs + 1
            continue
        file_count += 1
    return file_count, dir_count


def clear_data(root: Path) -> None:
    for child in root.iterdir():
        if child.is_dir():
            if child.name in KEEP_SUBDIRS:
                for item in child.iterdir():
                    if item.is_dir():
                        shutil.rmtree(item)
                    else:
                        item.unlink()
                continue
            shutil.rmtree(child)
            continue
        child.unlink()
    for name in KEEP_SUBDIRS:
        (root / name).mkdir(parents=True, exist_ok=True)


def main() -> None:
    if not DATA_DIR.exists():
        print(f"目录不存在: {DATA_DIR}")
        return
    if not DATA_DIR.is_dir():
        raise ValueError(f"路径不是目录: {DATA_DIR}")
    file_count, dir_count = compute_delete_counts(DATA_DIR)
    if file_count == 0 and dir_count == 0:
        print("未找到可删除的数据")
        for name in KEEP_SUBDIRS:
            (DATA_DIR / name).mkdir(parents=True, exist_ok=True)
        return
    print("将清空 data/src、data/dwd、data/dws 下的所有内容，并删除 data/ 下其他目录与文件")
    print(f"待删除文件数: {file_count}")
    print(f"待删除目录数: {dir_count}")
    confirm = input("确认请输入 YES（大小写不敏感）: ").strip().lower()
    if confirm != "yes":
        print("已取消")
        return
    clear_data(DATA_DIR)
    print("已清理完成（已保留 data/src、data/dwd、data/dws 目录）")


if __name__ == "__main__":
    sys.exit(main())
