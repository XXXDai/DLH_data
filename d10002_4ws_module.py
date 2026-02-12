from pathlib import Path
import runpy


def run() -> None:
    entry_path = Path(__file__).resolve().parent / "D10002-4" / "d10002-4ws.py"
    runpy.run_path(str(entry_path), run_name="__main__")


def main() -> None:
    run()
