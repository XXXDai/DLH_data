from pathlib import Path
import runpy


def run() -> None:
    entry_path = Path(__file__).resolve().parent / "D10006-8" / "d10006-8ws.py"
    runpy.run_path(str(entry_path), run_name="__main__")


def main() -> None:
    run()
