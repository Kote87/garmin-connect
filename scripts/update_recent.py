#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import pandas as pd


def run(cmd):
    print(">>", " ".join(str(c) for c in cmd))
    subprocess.run(cmd, check=True)


def find_repo_root(start: Path) -> Path:
    current = start.resolve()
    for parent in [current, *current.parents]:
        if (parent / ".git").exists() or (parent / "requirements.txt").is_file():
            return parent
    raise SystemExit("No encuentro ROOT (buscaba .git o requirements.txt)")


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def last_date_from_processed(processed_dir: Path) -> Optional[date]:
    daily_path = processed_dir / "daily.parquet"
    if not daily_path.exists():
        return None
    try:
        daily = pd.read_parquet(daily_path)
    except Exception:
        return None
    if "date" not in daily.columns or daily.empty:
        return None
    dates = pd.to_datetime(daily["date"], errors="coerce").dt.date
    dates = dates.dropna()
    if dates.empty:
        return None
    return max(dates)


def last_date_from_raw(raw_dir: Path) -> Optional[date]:
    dates = []
    for path in raw_dir.glob("????-??-??_*.json"):
        name = path.name.split("_")[0]
        try:
            dates.append(parse_date(name))
        except ValueError:
            continue
    return max(dates) if dates else None


def resolve_end_date(end_value: str, tz: str) -> date:
    if end_value == "yesterday":
        return (datetime.now(ZoneInfo(tz)).date() - timedelta(days=1))
    return parse_date(end_value)


def download_chunks(extract_script: Path, raw_dir: Path, start: date, end: date, chunk_days: int) -> None:
    chunk = max(1, chunk_days)
    current = start
    while current <= end:
        chunk_end = min(end, current + timedelta(days=chunk - 1))
        run([
            sys.executable,
            str(extract_script),
            "--start",
            current.isoformat(),
            "--end",
            chunk_end.isoformat(),
            "--out",
            str(raw_dir),
        ])
        current = chunk_end + timedelta(days=1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tz", type=str, default="Europe/Madrid")
    ap.add_argument("--raw", type=str, default="data/raw")
    ap.add_argument("--processed", type=str, default="data/processed")
    ap.add_argument("--end", type=str, default="yesterday", help="yesterday|YYYY-MM-DD")
    ap.add_argument("--chunk-days", type=int, default=7)
    ap.add_argument("--rebuild", action=argparse.BooleanOptionalAction, default=True)
    args = ap.parse_args()

    root = find_repo_root(Path(__file__).resolve())
    extract = root / "scripts" / "extract_raw.py"
    build = root / "scripts" / "build_dataset.py"

    raw_dir = Path(args.raw).expanduser()
    processed_dir = Path(args.processed).expanduser()

    last_date = last_date_from_processed(processed_dir)
    if last_date is None:
        last_date = last_date_from_raw(raw_dir)

    end_date = resolve_end_date(args.end, args.tz)
    start_date = end_date if last_date is None else last_date + timedelta(days=1)

    if start_date <= end_date:
        print(f"Descarga pendiente: {start_date} -> {end_date}")
        download_chunks(extract, raw_dir, start_date, end_date, args.chunk_days)
    else:
        print(f"Nada que descargar (last_date={last_date}, end_date={end_date})")

    if args.rebuild:
        run([
            sys.executable,
            str(build),
            "--raw",
            str(raw_dir),
            "--out",
            str(processed_dir),
            "--tz",
            args.tz,
            "--drop-empty-days",
        ])

    print("✅ update_recent terminado")


if __name__ == "__main__":
    main()
