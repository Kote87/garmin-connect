#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Descarga RAW de Garmin Connect (JSON) por día y Body Battery por rango.

Guarda en data/raw/:
- YYYY-MM-DD_heart_rates.json
- YYYY-MM-DD_stress.json
- YYYY-MM-DD_respiration.json
- YYYY-MM-DD_sleep.json
- YYYY-MM-DD_user_summary.json
y body_battery_START_END.json

Auth robusta:
- Usa tokenstore (default ~/.garth).
- Si no existen oauth1_token.json + oauth2_token.json, hace login con garth y los guarda.
"""

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

TOKEN_FILES = ("oauth1_token.json", "oauth2_token.json")


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


def safe_call(fn, *args, retries=3, sleep_base=1.3):
    err = None
    for i in range(retries + 1):
        try:
            return fn(*args)
        except (GarminConnectTooManyRequestsError, GarminConnectConnectionError, GarthHTTPError) as e:
            err = e
            time.sleep(sleep_base * (i + 1))
    raise err


def tokenstore_has_tokens(tokenstore: Path) -> bool:
    for name in TOKEN_FILES:
        p = tokenstore / name
        if not p.is_file():
            return False
        if p.stat().st_size <= 0:
            return False
    return True


def init_api(tokenstore: Path) -> Garmin:
    tokenstore = tokenstore.expanduser()
    ensure_dir(tokenstore)

    # Ayuda a librerías que miran esta env var
    os.environ.setdefault("GARMINTOKENS", str(tokenstore))

    # 1) Si hay tokens, intenta cargar sesión directamente
    if tokenstore_has_tokens(tokenstore):
        g = Garmin()
        g.login(str(tokenstore))
        return g

    # 2) Si no hay tokens, login con garth (una vez) y guardarlos
    email = os.getenv("EMAIL") or input("Login email: ").strip()
    password = os.getenv("PASSWORD") or getpass("Password: ")

    import garth
    garth.login(email, password, prompt_mfa=lambda: input("MFA code: ").strip())
    garth.save(str(tokenstore))

    # 3) Ya con tokens en disco, carga sesión Garmin
    g = Garmin()
    g.login(str(tokenstore))
    return g


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="Últimos N días (incluye hoy)")
    ap.add_argument("--start", type=str, default=None, help="YYYY-MM-DD (opcional)")
    ap.add_argument("--end", type=str, default=None, help="YYYY-MM-DD (opcional)")
    ap.add_argument("--out", type=str, default="data/raw", help="Carpeta salida RAW")
    ap.add_argument("--tokenstore", type=str, default=os.path.expanduser("~/.garth"), help="Carpeta tokens")
    ap.add_argument("--pause", type=float, default=0.25, help="Pausa entre días")
    ap.add_argument("--force", action="store_true", help="Re-descarga aunque el JSON ya exista")
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

    # Body Battery (por rango)
    bb_path = outdir / f"body_battery_{start}_{end}.json"
    bb_err_path = outdir / f"body_battery_error_{start}_{end}.json"
    if args.force or not bb_path.exists():
        try:
            bb = safe_call(api.get_body_battery, start.isoformat(), end.isoformat())
            dump_json(bb_path, bb)
        except Exception as e:
            dump_json(bb_err_path, {"error": str(e)})

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
            err_path = outdir / f"{ds}_{name}_error.json"

            if not args.force and path.exists():
                continue

            try:
                data = safe_call(fn, ds)
                dump_json(path, data)
                # si había error previo, lo dejamos (no molesta) o podrías borrarlo si quieres
            except Exception as e:
                dump_json(err_path, {"error": str(e)})

        time.sleep(args.pause)

    print("OK: RAW descargado en", outdir)


if __name__ == "__main__":
    main()
