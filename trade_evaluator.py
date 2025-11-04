import yfinance as yf
from datetime import datetime
from positions_store import get_position, upsert_position, close_position, update_position

def _round2(x): 
    return None if x is None else round(float(x), 4)

def _last_price_yf(symbol):
    """Obtiene último precio minuto a minuto desde Yahoo Finance."""
    try:
        h = yf.Ticker(symbol).history(period="1d", interval="1m")
        if h is not None and not h.empty:
            return float(h["Close"].iloc[-1])
    except Exception:
        pass
    return None


# === 1️⃣ REGISTRO DE NUEVA SEÑAL ===
def register_new_signal(symbol, price, settings):
    risk = settings.get("risk", {})
    cap = float(risk.get("capital_per_trade_usd", 100))
    sl  = float(risk.get("stop_loss_pct", 8))
    tp1 = float(risk.get("tp1_pct", 10))
    tp2 = float(risk.get("tp2_pct", 20))
    pos = get_position(symbol)
    if pos and str(pos.get("status","")).startswith("OPEN"):
        return  # ya registrada

    stop = price * (1 - sl/100)
    p1   = price * (1 + tp1/100)
    p2   = price * (1 + tp2/100)
    upsert_position({
        "symbol": symbol,
        "status": "OPEN",
        "created_ts": datetime.now().isoformat(timespec="seconds"),
        "updated_ts": datetime.now().isoformat(timespec="seconds"),
        "entry_price": _round2(price),
        "avg_price": _round2(price),
        "qty_usd": cap,
        "adds_done": 0,
        "stop": _round2(stop),
        "tp1": _round2(p1),
        "tp2": _round2(p2),
        "partial_taken": False,
        "notes": ""
    })


# === 2️⃣ EVALUACIÓN DE POSICIÓN (para señales abiertas) ===
def evaluate_symbol(sym, scan_row, settings, alert):
    """Evalúa ganancia/pérdida actual y envía sugerencias dinámicas."""
    try:
        pos = get_position(sym)
        if not pos or str(pos.get("status", "")) != "OPEN":
            return

        entry_price = float(pos["entry_price"])
        price_now = float(scan_row["price"]) if scan_row is not None else entry_price
        pct_now = ((price_now - entry_price) / entry_price) * 100.0
        vol_now = float(scan_row["volume"]) if scan_row is not None else 0.0
        vol_rel = min(vol_now / (pos.get("avg_volume", vol_now) or 1), 3.0)

        msg = None

        if pct_now >= 5.0:
            msg = f"✅ {sym} +{pct_now:.2f}% — Considera tomar profit parcial o cerrar posición."
        elif -5.0 <= pct_now <= -3.0 and vol_rel >= 0.8:
            msg = f"⚖️ {sym} {pct_now:.2f}% — Volumen sostiene. Considera promediar posición."
        elif pct_now < -6.0 or vol_rel < 0.5:
            msg = f"❌ {sym} {pct_now:.2f}% — Debilidad confirmada. Sugerencia: cerrar posición."

        if msg:
            print(msg)
            alert.send_async_message(msg)
            update_position(sym, {"last_eval": datetime.now().isoformat(), "last_pct": pct_now})

    except Exception as e:
        print(f"⚠️ Error evaluando {sym}: {e}")


# === 3️⃣ MANEJO COMPLETO DE TP / STOP / ADD ===
def manage_trade(sym, scan_row, settings, alert):
    """Gestiona TP, SL y promedio de posiciones."""
    pos = get_position(sym)
    if not pos or not str(pos.get("status","")).startswith("OPEN"):
        return  

    risk = settings.get("risk", {})
    add_usd   = float(risk.get("add_on_usd", 50))
    max_adds  = int(risk.get("max_adds", 1))
    add_lo    = float(risk.get("add_zone_low_pct", -6))
    add_hi    = float(risk.get("add_zone_high_pct", -3))
    sl_pct    = float(risk.get("stop_loss_pct", 8))
    tp1_pct   = float(risk.get("tp1_pct", 10))
    tp2_pct   = float(risk.get("tp2_pct", 20))

    avg = float(pos["avg_price"])
    stop = float(pos["stop"])
    tp1 = float(pos["tp1"])
    tp2 = float(pos["tp2"])
    partial_taken = bool(pos.get("partial_taken", False))
    adds_done = int(pos.get("adds_done", 0))

    price_now = _last_price_yf(sym)
    if price_now is None:
        return

    # STOP
    if price_now <= stop:
        close_position(sym, "STOP")
        alert.send_async_message(f"🔴 STOP — {sym}  Px:{price_now:.2f} ≤ Stop:{stop:.2f}  (−{sl_pct:.0f}%)")
        return

    # TP2
    if price_now >= tp2:
        close_position(sym, "TP2")
        alert.send_async_message(f"🟢 TAKE PROFIT — {sym}  Px:{price_now:.2f} ≥ TP2:{tp2:.2f}  (+{tp2_pct:.0f}%)")
        return

    # TP1 parcial
    if (not partial_taken) and price_now >= tp1:
        stop = avg
        upsert_position({
            **pos,
            "partial_taken": True,
            "stop": _round2(stop)
        })
        alert.send_async_message(f"🟢 TP1 — {sym} Parcial 50% en {price_now:.2f}. Stop sube a BE {avg:.2f}.")
        return

    # ADD (average down)
    if scan_row is not None:
        try:
            still_strong = float(scan_row["pct"]) > 5.0
        except Exception:
            still_strong = False
    else:
        still_strong = False

    draw_pct = (price_now - float(pos["entry_price"])) / float(pos["entry_price"]) * 100

    if (adds_done < max_adds) and still_strong and (add_lo <= draw_pct <= add_hi):
        new_qty = float(pos["qty_usd"]) + add_usd
        new_avg = (avg * float(pos["qty_usd"]) + price_now * add_usd) / new_qty
        stop = new_avg * (1 - sl_pct/100)
        tp1  = new_avg * (1 + tp1_pct/100)
        tp2  = new_avg * (1 + tp2_pct/100)
        upsert_position({
            **pos,
            "avg_price": _round2(new_avg),
            "qty_usd": new_qty,
            "adds_done": adds_done + 1,
            "stop": _round2(stop),
            "tp1": _round2(tp1),
            "tp2": _round2(tp2),
        })
        alert.send_async_message(f"➕ ADD — {sym} +${add_usd} a {price_now:.2f}. Nuevo avg:{new_avg:.2f} Stop:{stop:.2f}")
