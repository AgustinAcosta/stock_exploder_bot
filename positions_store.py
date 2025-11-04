import os, pandas as pd, json
from datetime import datetime

POS_CSV_DEFAULT = "data/logs/positions.csv"

def _ensure_parent(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)

def load_positions(csv_path=POS_CSV_DEFAULT):
    if not os.path.exists(csv_path):
        return pd.DataFrame(columns=[
            "symbol","status","created_ts","updated_ts",
            "entry_price","avg_price","qty_usd","adds_done",
            "stop","tp1","tp2","partial_taken","notes"
        ])
    df = pd.read_csv(csv_path)
    if "partial_taken" in df.columns:
        df["partial_taken"] = df["partial_taken"].fillna(False).astype(bool)
    return df

def save_positions(df, csv_path=POS_CSV_DEFAULT):
    _ensure_parent(csv_path)
    df.to_csv(csv_path, index=False)

def upsert_position(pos, csv_path=POS_CSV_DEFAULT):
    df = load_positions(csv_path)
    now = datetime.now().isoformat(timespec="seconds")
    pos["updated_ts"] = now
    if (df["symbol"] == pos["symbol"]).any():
        df.loc[df["symbol"] == pos["symbol"], list(pos.keys())] = list(pos.values())
    else:
        pos.setdefault("created_ts", now)
        df = pd.concat([df, pd.DataFrame([pos])], ignore_index=True)
    save_positions(df, csv_path)

def get_position(symbol, csv_path=POS_CSV_DEFAULT):
    df = load_positions(csv_path)
    m = df[df["symbol"] == symbol]
    return None if m.empty else m.iloc[0].to_dict()

def close_position(symbol, reason, csv_path=POS_CSV_DEFAULT):
    df = load_positions(csv_path)
    if (df["symbol"] == symbol).any():
        df.loc[df["symbol"] == symbol, "status"] = f"CLOSED:{reason}"
        df.loc[df["symbol"] == symbol, "updated_ts"] = datetime.now().isoformat(timespec="seconds")
        save_positions(df, csv_path)
        
def update_position(symbol, updates, csv_path=POS_CSV_DEFAULT):
    """
    Actualiza solo los campos indicados de una posición abierta.
    Ejemplo:
        update_position("HTZ", {"avg_price": 7.45, "stop": 6.88})
    """
    df = load_positions(csv_path)
    if not (df["symbol"] == symbol).any():
        return
    for k, v in updates.items():
        if k not in df.columns:
            continue
        df.loc[df["symbol"] == symbol, k] = v
    df.loc[df["symbol"] == symbol, "updated_ts"] = datetime.now().isoformat(timespec="seconds")
    save_positions(df, csv_path)