import csv
from pathlib import Path
from typing import Iterable, Sequence


def append_etf_changes(etf_name: str,
                       added: Sequence[str],
                       removed: Sequence[str],
                       timestamp: str) -> None:
    """Append ETF additions/removals to output/ETF-Changes.csv.

    - etf_name: short code like "BUZZ", "HDGE", "GRNY", "MTUM", "MMTM".
    - added/removed: iterables of ticker strings.
    - timestamp: string such as "YYYY-MM-DD HH:MM:SS"; only the date part is stored.
    """
    # Nothing to record if there were no changes
    if not added and not removed:
        return

    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / "output" / "ETF-Changes.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    date_part = timestamp.split()[0] if " " in timestamp else timestamp

    file_exists = csv_path.exists()
    need_header = (not file_exists) or csv_path.stat().st_size == 0

    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if need_header:
            writer.writerow(["ETF Name", "Stock Name", "Addition", "Removal", "Date"])

        for symbol in added:
            symbol = str(symbol).strip()
            if symbol:
                writer.writerow([etf_name, symbol, "Yes", "", date_part])

        for symbol in removed:
            symbol = str(symbol).strip()
            if symbol:
                writer.writerow([etf_name, symbol, "", "Yes", date_part])
