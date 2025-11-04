import os, pandas as pd
from datetime import datetime

def ensure_parent(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def append_signal_row(csv_path, row: dict):
    ensure_parent(csv_path)
    df = pd.DataFrame([row])
    if os.path.exists(csv_path):
        df.to_csv(csv_path, mode="a", index=False, header=False)
    else:
        df.to_csv(csv_path, index=False)

def load_today_last_alerts(csv_path, today_str):
    if not os.path.exists(csv_path):
        return {}
    try:
        df = pd.read_csv(csv_path)
        if df.empty: return {}
        df = df[df["date"] == today_str]
        # nos quedamos con la última alerta por símbolo
        df = df.sort_values("ts").drop_duplicates("symbol", keep="last")
        out = {}
        for _, r in df.iterrows():
            out[r["symbol"]] = {
                "last_pct": float(r["pct_change"]),
                "last_price": float(r["price"]),
                "last_ts": r["ts"]
            }
        return out
    except Exception:
        return {}

def summarize_today(csv_path, today_str):
    if not os.path.exists(csv_path):
        return None
    df = pd.read_csv(csv_path)
    if df.empty: return None
    df = df[df["date"] == today_str]
    if df.empty: return None
    # ranking por pct_change máximo observado por símbolo
    agg = (df.groupby("symbol")
             .agg(first_time=("ts","min"),
                  last_time=("ts","max"),
                  alerts=("symbol","count"),
                  max_pct=("pct_change","max"),
                  last_price=("price","last"))
             .reset_index()
           )
    agg = agg.sort_values(["max_pct","alerts"], ascending=[False, False]).head(15)
    return agg
