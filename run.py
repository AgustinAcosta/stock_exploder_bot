import asyncio, warnings, os, yaml, yfinance as yf, pandas as pd, requests
from datetime import datetime, timedelta
from alert_manager import AlertManager
from store import append_signal_row, load_today_last_alerts, summarize_today
from trade_evaluator import register_new_signal, evaluate_symbol
from positions_store import load_positions
import json
from positions_store import get_position



warnings.filterwarnings("ignore")

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "../config/settings.yaml")
with open(CONFIG_PATH, "r") as f:
    settings = yaml.safe_load(f)

alert = AlertManager(settings["telegram_token"], settings["telegram_chat_id"])

SCAN_INTERVAL = int(settings.get("updates", {}).get("scan_interval_sec", 180))
MIN_CHANGE = float(settings.get("updates", {}).get("min_change_pct", 2.0))
COOLDOWN_MIN = int(settings.get("updates", {}).get("realert_cooldown_min", 15))
TOP_N = int(settings.get("updates", {}).get("top_n", 5))
LOG_CSV = settings.get("logging", {}).get("log_csv", "data/logs/signals.csv")

def now_str():
    return datetime.now().strftime("%H:%M:%S")

def today_str():
    return datetime.now().strftime("%Y-%m-%d")

async def scan_market_top_pennies():
    """Escáner robusto que usa los campos disponibles según el horario."""
    try:
        urls = [
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?count=100&scrIds=day_gainers",
            "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved?count=100&scrIds=most_actives"
        ]

        frames = []
        for url in urls:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
            if resp.status_code != 200:
                print(f"⚠️ Yahoo devolvió código {resp.status_code} para {url}")
                continue

            try:
                data = resp.json()
                quotes = data.get("finance", {}).get("result", [{}])[0].get("quotes", [])
                if not quotes:
                    continue
                df = pd.DataFrame(quotes)
                frames.append(df)
            except Exception as e:
                print(f"⚠️ Error decodificando JSON de Yahoo: {e}")
                continue

        if not frames:
            print("⚠️ Yahoo devolvió vacío para ambos endpoints.")
            return pd.DataFrame()

        df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["symbol"])

        # Buscar qué columnas existen según horario (market o postMarket) preMarketChangePercent
        possible_pct_cols = [
            "preMarketChangePercent", "postMarketChangePercent", "regularMarketChangePercent"
        ]
        possible_price_cols = [
            "regularMarketPrice", "postMarketPrice", "preMarketPrice"
        ]
        pct_col = next((c for c in possible_pct_cols if c in df.columns), None)
        price_col = next((c for c in possible_price_cols if c in df.columns), None)

        if not pct_col or not price_col:
            print("⚠️ Yahoo no tiene columnas válidas de precio/cambio.")
            return pd.DataFrame()

        # Definir columnas uniformes
        df["Symbol"] = df["symbol"]
        df["price"] = pd.to_numeric(df[price_col], errors="coerce")
        df["pct"] = pd.to_numeric(df[pct_col], errors="coerce")
        df["volume"] = pd.to_numeric(df.get("regularMarketVolume", df.get("postMarketVolume", df.get("preMarketVolume", 0))), errors="coerce")

        # Filtrar penny stocks de momentum
        print(df[["Symbol", "price", "pct", "volume"]].head(10))
        df = df[(df["price"] < 20.0) & (df["pct"] > 5.0) & (df["volume"] > 1_000_000)]
        if df.empty:
            print("⚠️ Ningún ticker cumplió los filtros actuales.")
            return pd.DataFrame()

        # ExplodeScore
        df["ExplodeScore"] = df["pct"] * 0.6 + (df["volume"] / df["volume"].max()) * 40.0
        df = df.sort_values("ExplodeScore", ascending=False).head(TOP_N).reset_index(drop=True)

        return df

    except Exception as e:
        print(f"❌ Error escaneando mercado: {e}")
        return pd.DataFrame()

async def main():
    start_msg = f"🟢 Stock Exploder Realtime iniciado — escaneo cada {SCAN_INTERVAL//60} min ⚡"
    print(start_msg)
    await alert.send_async_message(start_msg)

    # cache de última alerta por símbolo (cargamos lo de hoy del CSV si existe)
    last_alert = load_today_last_alerts(LOG_CSV, today_str())

    try:
        while True:
            df = await scan_market_top_pennies()
            ts = datetime.now().isoformat(timespec="seconds")
            dstr = today_str()

            if df is None or df.empty:
                print(f"[{now_str()}] ⚠️ Sin candidatos en este ciclo.")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            # 1) Cargar posiciones abiertas para NO repetir alertas
            try:
                pos_df = load_positions()
                open_symbols = (
                    pos_df[pos_df["status"].str.startswith("OPEN")]["symbol"].tolist()
                    if not pos_df.empty else []
                )
            except Exception as e:
                print(f"⚠️ No se pudieron cargar posiciones abiertas: {e}")
                open_symbols = []

            msgs = []

            # 2) Recorrer candidatos
            for _, r in df.iterrows():
                sym = r["Symbol"]
                price = float(r["price"])
                pct = float(r["pct"])
                vol = int(r["volume"])

                # Evitar alertas duplicadas si ya hay una posición abierta
                if sym in open_symbols:
                    # Solo log histórico
                    append_signal_row(LOG_CSV, {
                        "date": dstr, "ts": ts, "symbol": sym,
                        "price": price, "pct_change": pct, "volume": vol
                    })
                    continue

                # política de re-alerta (cooldown o salto de %)
                la = last_alert.get(sym)
                should_alert = False
                reason = "new"

                if la is None:
                    should_alert = True
                else:
                    delta_pct = pct - float(la.get("last_pct", 0.0))
                    last_time = datetime.fromisoformat(la["last_ts"])
                    minutes_passed = (datetime.now() - last_time).total_seconds() / 60.0
                    if delta_pct >= MIN_CHANGE or minutes_passed >= COOLDOWN_MIN:
                        should_alert = True
                        reason = f"+{delta_pct:.1f}% / {minutes_passed:.0f}m"

                # log histórico SIEMPRE
                append_signal_row(LOG_CSV, {
                    "date": dstr, "ts": ts, "symbol": sym,
                    "price": price, "pct_change": pct, "volume": vol
                })

                # 3) Construir mensaje SOLO si debemos alertar
                if should_alert:
                    # cálculo de sugerencia de acciones (fijo $100)
                    investment = float(settings.get("capital", {}).get("per_stock_usd", 100))
                    shares = max(1, int(investment // price))
                    total_cost = round(shares * price, 2)

                    # mensaje legible (1 sola entrada por símbolo)
                    msg = (
                        f"💎 {sym}\n"
                        f"📈 Cambio: +{pct:.2f}%\n"
                        f"💰 Precio: ${price:.2f}\n"
                        f"📊 Volumen: {vol:,}\n"
                        f"🎯 Acciones sugeridas: {shares} (~${total_cost})"
                    )
                    msgs.append(msg)

                    # actualizar memoria y registrar posición si es NEW
                    last_alert[sym] = {"last_pct": pct, "last_price": price, "last_ts": ts}
                    if reason == "new":
                        register_new_signal(sym, price, settings)

            # 4) Enviar batch del ciclo (si hubo algo)
            if msgs:
                header = f"🚀 [{now_str()}] Oportunidades long (low-price):\n"
                body = "\n\n".join(msgs)
                final = f"{header}{body}"
                print(final)
                await alert.send_async_message(final)
            else:
                print(f"[{now_str()}] ℹ️ Sin cambios significativos vs. últimas alertas.")

            # 5) Evaluar posiciones abiertas (ADD / TP / STOP)
            try:
                pos_df = load_positions()
                if not pos_df.empty:
                    for sym in pos_df[pos_df["status"].str.startswith("OPEN")]["symbol"]:
                        scan_row = None
                        if df is not None and not df.empty:
                            m = df[df["Symbol"] == sym]
                            scan_row = None if m.empty else m.iloc[0]
                        evaluate_symbol(sym, scan_row, settings, alert)
                        manage_trade(sym, scan_row, settings, alert)
            except Exception as e:
                print(f"⚠️ Error evaluando posiciones: {e}")

            await asyncio.sleep(SCAN_INTERVAL)

    except (KeyboardInterrupt, asyncio.CancelledError):
        # EOD summary
        try:
            summary = summarize_today(LOG_CSV, today_str())
            if summary is not None and not summary.empty:
                lines = []
                for _, r in summary.iterrows():
                    lines.append(f"{r['symbol']}: max {r['max_pct']:.1f}% | alerts {int(r['alerts'])}")
                msg = "📊 EOD — Resumen del día (máximo % change observado):\n" + "\n".join(lines)
                print(msg)
                await alert.send_async_message(msg)
            else:
                print("📊 EOD — Sin datos para resumir hoy.")
                await alert.send_async_message("📊 EOD — Sin datos para resumir hoy.")
        finally:
            print("⏹️ Bot detenido por el usuario.")
            await alert.send_async_message("⏹️ Bot detenido por el usuario.")
    start_msg = f"🟢 Stock Exploder Realtime iniciado — escaneo cada {SCAN_INTERVAL//60} min ⚡"
    print(start_msg)
    await alert.send_async_message(start_msg)

    # cache de última alerta por símbolo (cargamos lo de hoy del CSV si existe)
    last_alert = load_today_last_alerts(LOG_CSV, today_str())

    try:
        while True:
            df = await scan_market_top_pennies()
            ts = datetime.now().isoformat(timespec="seconds")
            dstr = today_str()

            if df is None or df.empty:
                print(f"[{now_str()}] ⚠️ Sin candidatos en este ciclo.")
                await asyncio.sleep(SCAN_INTERVAL)
                continue

            # recorrer candidatos y decidir si alertar o solo loguear
            msgs = []
            for _, r in df.iterrows():
                sym = r["Symbol"]
                price = float(r["price"])
                pct = float(r["pct"])
                vol = int(r["volume"])

                 # Cálculo de tamaño de posición fijo: $100 por stock
                investment = float(settings.get("capital", {}).get("per_stock_usd", 100))
                shares = int(investment // price)
                total_cost = round(shares * price, 2)

                # política de re-alerta
                la = last_alert.get(sym)
                should_alert = False
                reason = "new"

                # Formato visual con emojis y saltos de línea
                msg = (
                    f"💎 {sym}\n"
                    f"📈 Cambio: +{pct:.2f}%\n"
                    f"💰 Precio: ${price:.2f}\n"
                    f"📊 Volumen: {vol:,}\n"
                    f"🎯 Acciones sugeridas: {shares} (~${total_cost})"
                )
                msgs.append(msg)

                # Detectar si ya hay una posición abierta para este símbolo
                existing_pos = get_position(sym)
                if existing_pos and str(existing_pos.get("status", "")).startswith("OPEN"):
                    reason = "update"
                    should_alert = False  # no queremos repetir alerta
                    # Pero sí podemos actualizar el CSV con nuevo % para histórico
                    row = {
                        "date": dstr,
                        "ts": ts,
                        "symbol": sym,
                        "price": price,
                        "pct_change": pct,
                        "volume": vol
                    }
                    append_signal_row(LOG_CSV, row)
                    continue  # pasamos al siguiente símbolo
                if la is None:
                    should_alert = True
                else:
                    # cambio en pct desde la última alerta
                    delta_pct = pct - float(la.get("last_pct", 0.0))
                    # cooldown
                    last_time = datetime.fromisoformat(la["last_ts"])
                    minutes_passed = (datetime.now() - last_time).total_seconds() / 60.0
                    if delta_pct >= MIN_CHANGE or minutes_passed >= COOLDOWN_MIN:
                        should_alert = True
                        reason = f"+{delta_pct:.1f}% / {minutes_passed:.0f}m"

                # siempre registramos fila en CSV, alertemos o no
                row = {
                    "date": dstr,
                    "ts": ts,
                    "symbol": sym,
                    "price": price,
                    "pct_change": pct,
                    "volume": vol
                }
                append_signal_row(LOG_CSV, row)

                if should_alert:
                    last_alert[sym] = {"last_pct": pct, "last_price": price, "last_ts": ts}
                    msgs.append(f"• {sym}  {pct:.1f}%  Vol: {vol:,}  Px: ${price:.2f}  ({reason})")

                    if reason == "new":
                        # registrar posición virtual
                        register_new_signal(sym, price, settings)

            if msgs:
                header = f"🚀 [{now_str()}] Oportunidades long (low-price):\n"
                body = "\n\n".join(msgs)  # salto doble entre señales
                final = f"{header}{body}"
                print(final)
                await alert.send_async_message(final)
            else:
                print(f"[{now_str()}] ℹ️ Sin cambios significativos vs. últimas alertas.")

            # 🧠 Aquí insertas el bloque evaluador
            try:
                pos_df = load_positions()
                if not pos_df.empty:
                    for sym in pos_df[pos_df["status"].str.startswith("OPEN")]["symbol"]:
                        scan_row = None
                        if df is not None and not df.empty:
                            m = df[df["Symbol"] == sym]
                            scan_row = None if m.empty else m.iloc[0]
                        evaluate_symbol(sym, scan_row, settings, alert)
            except Exception as e:
                print(f"⚠️ Error evaluando posiciones: {e}")

            await asyncio.sleep(SCAN_INTERVAL)

    except (KeyboardInterrupt, asyncio.CancelledError):

        # EOD summary
        try:
            summary = summarize_today(LOG_CSV, today_str())
            if summary is not None and not summary.empty:
                lines = []
                for _, r in summary.iterrows():
                    lines.append(f"{r['symbol']}: max {r['max_pct']:.1f}% | alerts {int(r['alerts'])}")
                msg = "📊 EOD — Resumen del día (máximo % change observado):\n" + "\n".join(lines)
                print(msg)
                await alert.send_async_message(msg)
            else:
                print("📊 EOD — Sin datos para resumir hoy.")
                await alert.send_async_message("📊 EOD — Sin datos para resumir hoy.")
        finally:
            print("⏹️ Bot detenido por el usuario.")
            await alert.send_async_message("⏹️ Bot detenido por el usuario.")

if __name__ == "__main__":
    asyncio.run(main())
