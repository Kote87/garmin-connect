#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
import pandas as pd

TZ = "Europe/Madrid"

def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))

def ts_df_from_pairs(pairs, col):
    if not pairs:
        return pd.DataFrame(columns=[col], index=pd.DatetimeIndex([], name="timestamp"))
    df = pd.DataFrame(pairs, columns=["ts_ms", col])
    df = df.dropna(subset=["ts_ms"])
    df["timestamp"] = pd.to_datetime(df["ts_ms"].astype("int64"), unit="ms", utc=True).dt.tz_convert(TZ)
    df = df.drop(columns=["ts_ms"]).set_index("timestamp").sort_index()
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df[~df.index.duplicated(keep="last")]
    return df[[col]]

def extract_pairs_generic(obj):
    if isinstance(obj, dict):
        for k in [
            "heartRateValues", "heartRateValuesArray",
            "stressValuesArray", "stressValues",
            "respirationValuesArray", "respirationValues",
            "values",
        ]:
            v = obj.get(k)
            if isinstance(v, list) and v and isinstance(v[0], (list, tuple)) and len(v[0]) >= 2:
                return v
    return []

def extract_bb_from_bodybattery_json(bb_obj):
    pairs=[]
    if isinstance(bb_obj, list):
        for day in bb_obj:
            if isinstance(day, dict):
                arr = day.get("bodyBatteryValuesArray")
                if isinstance(arr, list):
                    for e in arr:
                        if isinstance(e,(list,tuple)) and len(e)>=2 and e[1] is not None:
                            pairs.append([e[0], e[1]])
    elif isinstance(bb_obj, dict):
        arr = bb_obj.get("bodyBatteryValuesArray")
        if isinstance(arr, list):
            for e in arr:
                if isinstance(e,(list,tuple)) and len(e)>=2 and e[1] is not None:
                    pairs.append([e[0], e[1]])
    return pairs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", default="data/raw", help="Carpeta RAW")
    ap.add_argument("--out", default="data/processed", help="Carpeta processed")
    ap.add_argument("--freq", default="1min", help="Frecuencia destino (1min, 5min, 15min...)")
    args = ap.parse_args()

    raw = Path(args.raw)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    days = sorted({p.name.split("_")[0] for p in raw.glob("????-??-??_heart_rates.json")})
    if not days:
        raise SystemExit("No encuentro RAW tipo YYYY-MM-DD_heart_rates.json en data/raw")

    hr_frames=[]
    st_frames=[]
    rp_frames=[]
    sleep_intervals=[]
    daily_rows=[]

    bb_series = pd.DataFrame(columns=["bb"], index=pd.DatetimeIndex([], name="timestamp"))
    bb_files = sorted(raw.glob("body_battery_*.json"))
    if bb_files:
        bb_obj = load_json(bb_files[0])
        bb_pairs = extract_bb_from_bodybattery_json(bb_obj)
        bb_series = ts_df_from_pairs(bb_pairs, "bb")

    for ds in days:
        hr_day = ts_df_from_pairs(extract_pairs_generic(load_json(raw / f"{ds}_heart_rates.json")), "hr")
        st_day = ts_df_from_pairs(extract_pairs_generic(load_json(raw / f"{ds}_stress.json")), "stress")
        rp_day = ts_df_from_pairs(extract_pairs_generic(load_json(raw / f"{ds}_respiration.json")), "resp")
        if not hr_day.empty: hr_frames.append(hr_day)
        if not st_day.empty: st_frames.append(st_day)
        if not rp_day.empty: rp_frames.append(rp_day)

        us = load_json(raw / f"{ds}_user_summary.json")
        steps = us.get("totalSteps") if isinstance(us, dict) else None
        kcal = us.get("totalKilocalories") if isinstance(us, dict) else None

        sl = load_json(raw / f"{ds}_sleep.json")
        sleep_flag = 1 if sl else 0

        def find_key(obj, key):
            if isinstance(obj, dict):
                if key in obj: return obj[key]
                for v in obj.values():
                    r = find_key(v, key)
                    if r is not None: return r
            elif isinstance(obj, list):
                for v in obj:
                    r = find_key(v, key)
                    if r is not None: return r
            return None

        s = find_key(sl, "sleepStartTimestampGMT")
        e = find_key(sl, "sleepEndTimestampGMT")
        if isinstance(s,(int,float)) and isinstance(e,(int,float)):
            sleep_intervals.append((int(s), int(e)))

        bb_val=None
        if not bb_series.empty:
            day_start = pd.Timestamp(ds).tz_localize(TZ)
            day_end = day_start + pd.Timedelta(days=1)
            slc = bb_series.loc[(bb_series.index >= day_start) & (bb_series.index < day_end)]
            if not slc.empty and not slc["bb"].dropna().empty:
                bb_val=float(slc["bb"].dropna().iloc[-1])

        daily_rows.append({
            "date": ds,
            "hr": float(hr_day["hr"].mean()) if not hr_day.empty else None,
            "stress": float(st_day["stress"].mean()) if not st_day.empty else None,
            "bb": bb_val,
            "resp": float(rp_day["resp"].mean()) if not rp_day.empty else None,
            "steps": steps,
            "kcal": kcal,
            "sleep_flag": sleep_flag,
        })

    start = pd.Timestamp(days[0]).tz_localize(TZ)
    end = (pd.Timestamp(days[-1]).tz_localize(TZ) + pd.Timedelta(days=1)) - pd.Timedelta(minutes=1)
    idx = pd.date_range(start=start, end=end, freq=args.freq)
    base = pd.DataFrame(index=idx)

    def concat(frames, col):
        if not frames:
            return pd.DataFrame(columns=[col], index=pd.DatetimeIndex([], name="timestamp"))
        df = pd.concat(frames).sort_index()
        df = df[~df.index.duplicated(keep="last")]
        return df[[col]]

    base = base.join(concat(hr_frames, "hr"), how="left")
    base = base.join(concat(st_frames, "stress"), how="left")
    base = base.join(concat(rp_frames, "resp"), how="left")
    if not bb_series.empty:
        base = base.join(bb_series, how="left")

    for c in ["hr","stress","resp","bb"]:
        if c in base.columns:
            base[c] = base[c].ffill()

    daily = pd.DataFrame(daily_rows).sort_values("date")
    daily2 = daily.copy()
    daily2["day"] = pd.to_datetime(daily2["date"]).dt.tz_localize(TZ)
    daily2 = daily2.set_index("day")[["steps","kcal"]]
    base["day"] = base.index.floor("D")
    base = base.join(daily2, on="day").drop(columns=["day"])

    base["sleep_flag"] = 0
    for s_ms, e_ms in sleep_intervals:
        s = pd.to_datetime(s_ms, unit="ms", utc=True).tz_convert(TZ)
        e = pd.to_datetime(e_ms, unit="ms", utc=True).tz_convert(TZ)
        s2 = max(s, base.index[0]); e2 = min(e, base.index[-1])
        if e2 >= s2:
            base.loc[(base.index >= s2) & (base.index <= e2), "sleep_flag"] = 1

    minute_out = base.reset_index().rename(columns={"index":"timestamp"})
    minute_out.to_parquet(out / "minute.parquet", index=False)
    daily.to_parquet(out / "daily.parquet", index=False)

    print("OK:")
    print("-", out / "minute.parquet")
    print("-", out / "daily.parquet")

if __name__ == "__main__":
    main()
