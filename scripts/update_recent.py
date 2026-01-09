#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path


def run(cmd):
    print(">>", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="Cuántos días recientes refrescar")
    ap.add_argument("--raw", type=str, default="data/raw")
    ap.add_argument("--out", type=str, default="data/processed")
    ap.add_argument("--tokenstore", type=str, default=str(Path("~/.garth").expanduser()))
    ap.add_argument("--pause", type=float, default=0.25)
    ap.add_argument("--freq", type=str, default="1min")
    ap.add_argument("--tz", type=str, default="Europe/Madrid")
    ap.add_argument("--bb-fill", type=str, default="ffill_bfill", choices=["none", "ffill", "ffill_bfill", "interpolate"])
    ap.add_argument("--write-csv", action="store_true")
    args = ap.parse_args()

    end = date.today()
    start = end - timedelta(days=max(1, args.days) - 1)

    root = Path(__file__).resolve().parents[1]
    extract = root / "scripts" / "extract_raw.py"
    build = root / "scripts" / "build_dataset.py"

    run([
        sys.executable, str(extract),
        "--start", start.isoformat(),
        "--end", end.isoformat(),
        "--out", args.raw,
        "--tokenstore", args.tokenstore,
        "--pause", str(args.pause),
        "--force",
    ])

    cmd = [
        sys.executable, str(build),
        "--raw", args.raw,
        "--out", args.out,
        "--freq", args.freq,
        "--tz", args.tz,
        "--bb-fill", args.bb_fill,
        "--drop-empty-days",
    ]
    if args.write_csv:
        cmd.append("--write-csv")
    run(cmd)

    print("✅ update_recent terminado:", start, "->", end)


if __name__ == "__main__":
    main()
