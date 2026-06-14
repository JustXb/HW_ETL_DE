from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SOURCE = ROOT / "transactions_v2.csv"
TARGET = ROOT / "transactions_v2_ydb.csv"


def to_unix_seconds(value: str) -> int:
    parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    return int(parsed.replace(tzinfo=timezone.utc).timestamp())


def main() -> None:
    with SOURCE.open("r", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)

        with TARGET.open("w", encoding="utf-8", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                row["call_time"] = to_unix_seconds(row["call_time"])
                writer.writerow(row)

    print(f"Prepared {TARGET}")


if __name__ == "__main__":
    main()
