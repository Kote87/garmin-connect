#!/usr/bin/env python3
import argparse
import json
import os
import time
from datetime import date, datetime, timedelta
from getpass import getpass
from pathlib import Path

from garth.exc import GarthHTTPError
from garminconnect import (
    Garmin,
    GarminConnectConnectionError,
    GarminConnectTooManyRequestsError,
)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def dump_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def daterange_inclusive(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def safe_call(fn, *args, retries=3, sleep_base=1.2):
    err = None
    for i in range(retries + 1):
        try:
            return fn(*args)
        except (GarminConnectTooManyRequestsError, GarminConnectConnectionError, GarthHTTPError) as e:
            err = e
            time.sleep(sleep_base * (i + 1))
    raise err

def init_api(tokenstore: Path) -> Garmin:
    ensure_dir(tokenstore)
    try:
        g = Garmin()
        g.login(str(tokenstore))
        return g
    except Exception:
        pass

    email = os.getenv("EMAIL") or input("Login email: ").strip()
    password = os.getenv("PASSWORD") or getpass("Password: ")

    g = Garmin(email=email, password=password, is_cn=False, return_on_mfa=True)
    r1, r2 = g.login()
    if r1 == "needs_mfa":
        code = input("MFA code: ").strip()
        g.resume_login(r2, code)

    g.garth.dump(str(tokenstore))
    return g

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="Últimos N días (incluye hoy)")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD (opcional)")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD (opcional)")
    ap.add_argument("--out", type=str, default="data/raw", help="Carpeta salida RAW")
    ap.add_argument("--tokenstore", type=str, default=os.path.expanduser("~/.garth"), help="Carpeta tokens")
    ap.add_argument("--pause", type=float, default=0.2, help="Pausa entre días")
    args = ap.parse_args()

    outdir = Path(args.out).expanduser()
    ensure_dir(outdir)

    if args.start and args.end:
        start = parse_date(args.start)
        end = parse_date(args.end)
    else:
        end = date.today()
        start = end - timedelta(days=max(1, args.days) - 1)

    print(f"Rango: {start} -> {end}")

    api = init_api(Path(args.tokenstore).expanduser())

    try:
        bb = safe_call(api.get_body_battery, start.isoformat(), end.isoformat())
        dump_json(outdir / f"body_battery_{start}_{end}.json", bb)
    except Exception as e:
        dump_json(outdir / f"body_battery_error_{start}_{end}.json", {"error": str(e)})

    endpoints = {
        "heart_rates": api.get_heart_rates,
        "stress": api.get_stress_data,
        "respiration": api.get_respiration_data,
        "sleep": api.get_sleep_data,
        "user_summary": api.get_user_summary,
    }

    for d in daterange_inclusive(start, end):
        ds = d.isoformat()
        for name, fn in endpoints.items():
            path = outdir / f"{ds}_{name}.json"
            if path.exists():
                continue
            try:
                data = safe_call(fn, ds)
                dump_json(path, data)
            except Exception as e:
                dump_json(outdir / f"{ds}_{name}_error.json", {"error": str(e)})
        time.sleep(args.pause)

    print("OK: RAW descargado en", outdir)

if __name__ == "__main__":
    main()
