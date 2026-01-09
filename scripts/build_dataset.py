#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Construye datasets limpios desde data/raw.

Outputs:
- data/processed/minute.parquet : 1 fila = 1 minuto (timestamp) con columnas:
  hr, stress, resp, bb, steps, kcal, sleep_flag
- data/processed/daily.parquet  : 1 fila = 1 día (date) con columnas resumen + coberturas.

Soporta:
- múltiples body_battery_*.json (los concatena)
- días vacíos (los descarta)
- relleno de BB dentro del día (ffill/bfill o interpolación)
"""

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def load_json(p: Path) -> Any:
    return json.loads(p.read_text(encoding="utf-8"))


def discover_days(raw_dir: Path) -> List[str]:
    days = set()
    for p in raw_dir.glob("????-??-??_*.json"):
        # evita *_error.json si quieres, pero no molesta
        day = p.name.split("_")[0]
        if day.count("-") == 2:
            days.add(day)
    return sorted(days)


def to_epoch_ms(x: float) -> int:
    v = int(x)
    return v * 1000 if v < 10_000_000_000 else v


def collect_pairs(obj: Any, value_min: float, value_max: float) -> List[Tuple[int, float]]:
    """
    Busca recursivamente listas de pares [timestamp,value].
    Filtra por rango de valores para evitar basura.
    """
    out: List[Tuple[int, float]] = []

    def is_epoch(v) -> bool:
        return isinstance(v, (int, float)) and v > 1e9

    def rec(o: Any):
        if isinstance(o, list):
            # caso: lista grande de pares
            if len(o) >= 10 and all(isinstance(e, (list, tuple)) and len(e) >= 2 for e in o[:10]):
                score = sum(1 for e in o[:10] if is_epoch(e[0]))
                if score >= 8:
                    for e in o:
                        ts, val = e[0], e[1]
                        if val is None:
                            continue
                        if not isinstance(val, (int, float)):
                            continue
                        v = float(val)
                        if value_min <= v <= value_max:
                            out.append((to_epoch_ms(ts), v))
                    return
            for e in o:
                rec(e)
        elif isinstance(o, dict):
            for v in o.values():
                rec(v)

    rec(obj)
    return out


def pairs_to_series(pairs: List[Tuple[int, float]], tz: str) -> pd.Series:
    if not pairs:
        return pd.Series(dtype="float64", index=pd.DatetimeIndex([], tz=tz, name="timestamp"))
    df = pd.DataFrame(pairs, columns=["ts", "value"]).dropna()
    ts = pd.to_datetime(df["ts"].astype("int64"), unit="ms", utc=True).dt.tz_convert(tz)
    s = pd.Series(df["value"].astype("float64").to_numpy(), index=ts).sort_index()
    s = s[~s.index.duplicated(keep="last")]
    return s


def load_body_battery_series(raw_dir: Path, tz: str) -> pd.Series:
    pairs_all: List[Tuple[int, float]] = []
    for p in sorted(raw_dir.glob("body_battery_*.json")):
        if "error" in p.name.lower():
            continue
        try:
            obj = load_json(p)
            pairs_all.extend(collect_pairs(obj, 0, 100))
        except Exception:
            continue
    return pairs_to_series(pairs_all, tz)


def find_first_number(obj: Any, keys: List[str]) -> Optional[float]:
    found: List[float] = []

    def rec(o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in keys and isinstance(v, (int, float)):
                    found.append(float(v))
                rec(v)
        elif isinstance(o, list):
            for e in o:
                rec(e)

    rec(obj)
    return found[0] if found else None


def parse_sleep_window(obj: Any, tz: str) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
    """
    Intenta encontrar sleepStartTimestamp* y sleepEndTimestamp* (ms o ISO) dentro del JSON.
    """
    start_keys = ["sleepStartTimestampGMT", "sleepStartTimestampLocal"]
    end_keys = ["sleepEndTimestampGMT", "sleepEndTimestampLocal"]

    def parse_ts(v) -> Optional[pd.Timestamp]:
        if isinstance(v, (int, float)):
            return pd.to_datetime(int(v), unit="ms", utc=True).tz_convert(tz)
        if isinstance(v, str) and v.strip():
            t = pd.to_datetime(v, errors="coerce")
            if pd.isna(t):
                return None
            if t.tzinfo is None:
                return t.tz_localize(tz)
            return t.tz_convert(tz)
        return None

    def rec(o: Any) -> Optional[Tuple[pd.Timestamp, pd.Timestamp]]:
        if isinstance(o, dict):
            for sk in start_keys:
                if sk in o:
                    for ek in end_keys:
                        if ek in o:
                            s = parse_ts(o[sk])
                            e = parse_ts(o[ek])
                            if s is not None and e is not None:
                                return (s, e)
            for v in o.values():
                r = rec(v)
                if r is not None:
                    return r
        elif isinstance(o, list):
            for e in o:
                r = rec(e)
                if r is not None:
                    return r
        return None

    return rec(obj)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=str, default="data/raw")
    ap.add_argument("--out", type=str, default="data/processed")
    ap.add_argument("--tz", type=str, default="Europe/Madrid")
    ap.add_argument("--freq", type=str, default="1min")
    ap.add_argument("--bb-fill", type=str, default="ffill_bfill", choices=["none", "ffill", "ffill_bfill", "interpolate"])
    ap.add_argument("--drop-empty-days", action="store_true", help="Descarta días sin datos útiles (recomendado)")
    ap.add_argument("--write-csv", action="store_true", help="Además de parquet, escribe CSV")
    args = ap.parse_args()

    raw_dir = Path(args.raw).expanduser()
    out_dir = Path(args.out).expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)

    days = discover_days(raw_dir)
    if not days:
        raise SystemExit(f"No encuentro días en {raw_dir} (esperaba ficheros YYYY-MM-DD_*.json)")

    bb_global = load_body_battery_series(raw_dir, args.tz)

    minute_frames: List[pd.DataFrame] = []
    daily_rows: List[Dict[str, Any]] = []

    for ds in days:
        day_start = pd.Timestamp(ds).tz_localize(args.tz)
        day_end = day_start + pd.Timedelta(days=1)
        # índice del día a freq
        day_idx = pd.date_range(
            start=day_start,
            end=day_end - pd.Timedelta(args.freq),
            freq=args.freq,
        )
        day_df = pd.DataFrame(index=day_idx)

        # Carga por día (si existe)
        p_hr = raw_dir / f"{ds}_heart_rates.json"
        p_st = raw_dir / f"{ds}_stress.json"
        p_re = raw_dir / f"{ds}_respiration.json"
        p_sl = raw_dir / f"{ds}_sleep.json"
        p_us = raw_dir / f"{ds}_user_summary.json"

        # Series temporales
        if p_hr.exists():
            s = pairs_to_series(collect_pairs(load_json(p_hr), 20, 250), args.tz)
            s = s[(s.index >= day_start) & (s.index < day_end)].resample(args.freq).mean()
            day_df["hr"] = s.reindex(day_idx)
        else:
            day_df["hr"] = pd.NA

        if p_st.exists():
            s = pairs_to_series(collect_pairs(load_json(p_st), 0, 100), args.tz)
            s = s[(s.index >= day_start) & (s.index < day_end)].resample(args.freq).mean()
            day_df["stress"] = s.reindex(day_idx)
        else:
            day_df["stress"] = pd.NA

        if p_re.exists():
            s = pairs_to_series(collect_pairs(load_json(p_re), 2, 60), args.tz)
            s = s[(s.index >= day_start) & (s.index < day_end)].resample(args.freq).mean()
            day_df["resp"] = s.reindex(day_idx)
        else:
            day_df["resp"] = pd.NA

        # Body Battery desde global
        if not bb_global.empty:
            s = bb_global[(bb_global.index >= day_start) & (bb_global.index < day_end)].resample(args.freq).mean()
            day_df["bb"] = s.reindex(day_idx)
        else:
            day_df["bb"] = pd.NA

        # Steps/Kcal (diarios)
        steps = None
        kcal = None
        if p_us.exists():
            us = load_json(p_us)
            steps = find_first_number(us, ["totalSteps", "steps"])
            kcal = find_first_number(us, ["totalKilocalories", "totalKiloCalories", "kilocalories", "kiloCalories"])

        day_df["steps"] = steps
        day_df["kcal"] = kcal

        # Sleep flag (ventana principal)
        sleep_flag = pd.Series(0, index=day_idx, dtype="int8")
        if p_sl.exists():
            sl = load_json(p_sl)
            win = parse_sleep_window(sl, args.tz)
            if win is not None:
                s0, s1 = win
                mask = (day_idx >= s0) & (day_idx < s1)
                sleep_flag.loc[mask] = 1
        day_df["sleep_flag"] = sleep_flag

        # Si el día está vacío (sin ninguna métrica temporal), lo descartamos
        core_cols = ["hr", "stress", "resp", "bb"]
        has_any = day_df[core_cols].notna().any().any()

        if args.drop_empty_days and not has_any:
            continue

        # Relleno BB dentro del día (opcional)
        day_df["bb"] = pd.to_numeric(day_df["bb"], errors="coerce")
        if args.bb_fill == "ffill":
            day_df["bb"] = day_df["bb"].ffill()
        elif args.bb_fill == "ffill_bfill":
            day_df["bb"] = day_df["bb"].ffill().bfill()
        elif args.bb_fill == "interpolate":
            # interpolación temporal dentro del día
            # Convertir a numérico para evitar NAType (pd.NA) antes de interpolar
            day_df["bb"] = pd.to_numeric(day_df["bb"], errors="coerce")
            day_df["bb"] = (
                day_df["bb"]
                .interpolate(method="time", limit_direction="both")
                .ffill()
                .bfill()
            )

        # Daily row
        hr_cov = float(day_df["hr"].notna().mean()) if "hr" in day_df else 0.0
        st_cov = float(day_df["stress"].notna().mean()) if "stress" in day_df else 0.0
        re_cov = float(day_df["resp"].notna().mean()) if "resp" in day_df else 0.0
        bb_cov = float(day_df["bb"].notna().mean()) if "bb" in day_df else 0.0

        daily_rows.append(
            {
                "date": ds,
                "hr": float(pd.to_numeric(day_df["hr"], errors="coerce").mean()) if day_df["hr"].notna().any() else None,
                "stress": float(pd.to_numeric(day_df["stress"], errors="coerce").mean()) if day_df["stress"].notna().any() else None,
                "resp": float(pd.to_numeric(day_df["resp"], errors="coerce").mean()) if day_df["resp"].notna().any() else None,
                "bb": float(pd.to_numeric(day_df["bb"], errors="coerce").dropna().iloc[-1]) if day_df["bb"].notna().any() else None,
                "steps": steps,
                "kcal": kcal,
                "sleep_flag": int(day_df["sleep_flag"].max()),
                "coverage_hr": hr_cov,
                "coverage_stress": st_cov,
                "coverage_resp": re_cov,
                "coverage_bb": bb_cov,
            }
        )

        minute_frames.append(day_df.reset_index(names="timestamp"))

    if not minute_frames:
        raise SystemExit("No se generó minute dataset (¿todo vacío?)")

    minute = pd.concat(minute_frames, ignore_index=True)
    daily = pd.DataFrame(daily_rows)
    daily["date"] = pd.to_datetime(daily["date"]).dt.date

    minute_path = out_dir / "minute.parquet"
    daily_path = out_dir / "daily.parquet"
    minute.to_parquet(minute_path, index=False)
    daily.to_parquet(daily_path, index=False)

    if args.write_csv:
        minute.to_csv(out_dir / "minute.csv", index=False)
        daily.to_csv(out_dir / "daily.csv", index=False)

    print("✅ Dataset construido")
    print("-", minute_path)
    print("-", daily_path)
    print("minute rows:", len(minute), "daily rows:", len(daily))


if __name__ == "__main__":
    main()
