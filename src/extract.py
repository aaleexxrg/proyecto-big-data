"""
extract.py — Módulo de Extracción de Fuentes
Pipeline ETL IBEX 35 | Introducción a los Sistemas Big Data 2025-2026

Fuentes:
  F1 — Precios históricos diarios IBEX 35 (Yahoo Finance / yfinance)
  F2 — Componentes y métricas fundamentales (Wikipedia scraping + manual)
  F3 — Dividendos históricos por empresa (yfinance)
  F4 — Noticias y sentimiento financiero (NewsAPI o datos sintéticos)
"""

import os
import json
import logging
import datetime
import time
import traceback

import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup

from tracker import Tracker

# ── Configuración de logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/extract.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ── Tickers IBEX 35 (35 empresas) ─────────────────────────────────────────────
IBEX35_TICKERS = {
    "ACS.MC":    "ACS",
    "ACX.MC":    "Acerinox",
    "AMS.MC":    "Amadeus",
    "ANA.MC":    "Acciona",
    "ANE.MC":    "Acciona Energía",
    "BBVA.MC":   "BBVA",
    "BKT.MC":    "Bankinter",
    "CABK.MC":   "CaixaBank",
    "CLNX.MC":   "Cellnex",
    "COL.MC":    "Inmobiliaria Colonial",
    "ELE.MC":    "Endesa",
    "ENG.MC":    "Enagás",
    "FDR.MC":    "Fluidra",
    "FER.MC":    "Ferrovial",
    "GRF.MC":    "Grifols",
    "IAG.MC":    "IAG",
    "IBE.MC":    "Iberdrola",
    "IDR.MC":    "Indra",
    "INDITEX.MC":"Inditex",
    "LOG.MC":    "Logista",
    "MAP.MC":    "MAPFRE",
    "MEL.MC":    "Meliá Hotels",
    "MRL.MC":    "Merlin Properties",
    "MTS.MC":    "ArcelorMittal",
    "NTGY.MC":   "Naturgy",
    "PUIG.MC":   "Puig Brands",
    "RED.MC":    "REE",
    "REP.MC":    "Repsol",
    "ROVI.MC":   "Laboratorios Rovi",
    "SAB.MC":    "Banco Sabadell",
    "SAN.MC":    "Banco Santander",
    "SGRE.MC":   "Siemens Gamesa",
    "SLR.MC":    "Solaria",
    "TEF.MC":    "Telefónica",
    "UNI.MC":    "Unicaja Banco",
}

SECTORES = {
    "ACS.MC": "Construcción",     "ACX.MC": "Materiales",
    "AMS.MC": "Tecnología",       "ANA.MC": "Utilities",
    "ANE.MC": "Energía",          "BBVA.MC": "Banca",
    "BKT.MC": "Banca",            "CABK.MC": "Banca",
    "CLNX.MC": "Telecomunicaciones","COL.MC": "Inmobiliario",
    "ELE.MC": "Utilities",        "ENG.MC": "Utilities",
    "FDR.MC": "Industria",        "FER.MC": "Construcción",
    "GRF.MC": "Salud",            "IAG.MC": "Transporte",
    "IBE.MC": "Utilities",        "IDR.MC": "Tecnología",
    "INDITEX.MC": "Consumo",      "LOG.MC": "Logística",
    "MAP.MC": "Seguros",          "MEL.MC": "Turismo",
    "MRL.MC": "Inmobiliario",     "MTS.MC": "Materiales",
    "NTGY.MC": "Utilities",       "PUIG.MC": "Consumo",
    "RED.MC": "Utilities",        "REP.MC": "Energía",
    "ROVI.MC": "Salud",           "SAB.MC": "Banca",
    "SAN.MC": "Banca",            "SGRE.MC": "Energía",
    "SLR.MC": "Energía",          "TEF.MC": "Telecomunicaciones",
    "UNI.MC": "Banca",
}

RAW_DIR = "data/raw"
START_DATE = "2020-01-01"
END_DATE   = datetime.date.today().strftime("%Y-%m-%d")


def extract_f1_precios(tracker: Tracker) -> pd.DataFrame:
    """F1 — Precios históricos diarios OHLCV (Open/High/Low/Close/Volume)."""
    log.info("=== F1: Extrayendo precios históricos IBEX 35 ===")
    frames = []
    failed = []

    for ticker, nombre in IBEX35_TICKERS.items():
        try:
            df = yf.download(ticker, start=START_DATE, end=END_DATE,
                             auto_adjust=True, progress=False)
            if df.empty:
                log.warning(f"  Sin datos para {ticker}")
                failed.append(ticker)
                continue

            # Aplanar MultiIndex si existe
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [col[0] for col in df.columns]

            df = df.reset_index()
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            df["ticker"] = ticker
            df["nombre_empresa"] = nombre
            df["sector"] = SECTORES.get(ticker, "Otro")
            frames.append(df)
            log.info(f"  ✓ {ticker}: {len(df)} registros")
            time.sleep(0.3)   # evitar rate-limit
        except Exception as e:
            log.error(f"  ✗ Error en {ticker}: {e}")
            failed.append(ticker)

    if not frames:
        raise RuntimeError("No se pudo descargar ningún ticker de F1")

    df_f1 = pd.concat(frames, ignore_index=True)
    df_f1.to_csv(f"{RAW_DIR}/f1_precios_raw.csv", index=False)
    log.info(f"F1 guardado: {len(df_f1)} registros, {len(df_f1.columns)} columnas")
    if failed:
        log.warning(f"F1 tickers fallidos: {failed}")

    tracker.log("EXTRACT", "F1_precios", len(df_f1), len(df_f1), 0,
                "Descarga OHLCV yfinance IBEX 35 2020-hoy")
    return df_f1


def extract_f2_fundamentales(tracker: Tracker) -> pd.DataFrame:
    """F2 — Métricas fundamentales: capitalización, PER, dividendo yield, beta."""
    log.info("=== F2: Extrayendo métricas fundamentales ===")
    records = []

    for ticker, nombre in IBEX35_TICKERS.items():
        row = {"ticker": ticker, "nombre_empresa": nombre,
               "sector": SECTORES.get(ticker, "Otro")}
        try:
            info = yf.Ticker(ticker).info
            row["market_cap"]          = info.get("marketCap")
            row["per"]                 = info.get("trailingPE")
            row["per_forward"]         = info.get("forwardPE")
            row["dividend_yield"]      = info.get("dividendYield")
            row["beta"]                = info.get("beta")
            row["price_to_book"]       = info.get("priceToBook")
            row["ebitda"]              = info.get("ebitda")
            row["revenue"]             = info.get("totalRevenue")
            row["profit_margin"]       = info.get("profitMargins")
            row["52w_high"]            = info.get("fiftyTwoWeekHigh")
            row["52w_low"]             = info.get("fiftyTwoWeekLow")
            row["avg_volume"]          = info.get("averageVolume")
            row["employees"]           = info.get("fullTimeEmployees")
            row["country"]             = info.get("country", "Spain")
            row["currency"]            = info.get("currency", "EUR")
            log.info(f"  ✓ {ticker}")
            time.sleep(0.5)
        except Exception as e:
            log.warning(f"  ✗ {ticker}: {e}")
        records.append(row)

    df_f2 = pd.DataFrame(records)
    df_f2.to_csv(f"{RAW_DIR}/f2_fundamentales_raw.csv", index=False)
    log.info(f"F2 guardado: {len(df_f2)} registros, {len(df_f2.columns)} columnas")

    tracker.log("EXTRACT", "F2_fundamentales", len(df_f2), len(df_f2), 0,
                "Métricas fundamentales yfinance.Ticker.info")
    return df_f2


def extract_f3_dividendos(tracker: Tracker) -> pd.DataFrame:
    """F3 — Historial de dividendos pagados por cada empresa."""
    log.info("=== F3: Extrayendo historial de dividendos ===")
    frames = []

    for ticker, nombre in IBEX35_TICKERS.items():
        try:
            divs = yf.Ticker(ticker).dividends
            if divs.empty:
                continue
            df_d = divs.reset_index()
            df_d.columns = ["fecha_ex_dividendo", "importe_dividendo"]
            df_d["ticker"] = ticker
            df_d["nombre_empresa"] = nombre
            df_d["sector"] = SECTORES.get(ticker, "Otro")
            df_d["fecha_ex_dividendo"] = df_d["fecha_ex_dividendo"].dt.tz_localize(None)
            frames.append(df_d)
            log.info(f"  ✓ {ticker}: {len(df_d)} dividendos")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  ✗ {ticker}: {e}")

    if not frames:
        log.warning("F3 sin datos de dividendos")
        df_f3 = pd.DataFrame(columns=["fecha_ex_dividendo","importe_dividendo",
                                       "ticker","nombre_empresa","sector"])
    else:
        df_f3 = pd.concat(frames, ignore_index=True)

    df_f3.to_csv(f"{RAW_DIR}/f3_dividendos_raw.csv", index=False)
    log.info(f"F3 guardado: {len(df_f3)} registros")

    tracker.log("EXTRACT", "F3_dividendos", len(df_f3), len(df_f3), 0,
                "Historial dividendos yfinance")
    return df_f3


def extract_f4_macro(tracker: Tracker) -> pd.DataFrame:
    """F4 — Indicadores macroeconómicos: tipo BCE, prima de riesgo, EUR/USD."""
    log.info("=== F4: Extrayendo datos macroeconómicos ===")
    records = []

    macro_series = {
        "EURUSD=X": "eur_usd",
        "^TNX":     "bono_eeuu_10y",
        "ES=F":     "futuro_ibex",
        "GC=F":     "oro_usd",
        "CL=F":     "petroleo_wti",
    }

    for ticker_m, col_name in macro_series.items():
        try:
            df_m = yf.download(ticker_m, start=START_DATE, end=END_DATE,
                               auto_adjust=True, progress=False)
            if df_m.empty:
                continue
            if isinstance(df_m.columns, pd.MultiIndex):
                df_m.columns = [c[0] for c in df_m.columns]
            df_m = df_m.reset_index()[["Date", "Close"]].copy()
            df_m.columns = ["fecha", col_name]
            df_m["fecha"] = pd.to_datetime(df_m["fecha"]).dt.tz_localize(None)
            records.append(df_m.set_index("fecha"))
            log.info(f"  ✓ {ticker_m} → {col_name}: {len(df_m)} registros")
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  ✗ {ticker_m}: {e}")

    if records:
        df_f4 = pd.concat(records, axis=1).reset_index()
        df_f4.rename(columns={"index": "fecha"}, inplace=True)
    else:
        df_f4 = pd.DataFrame(columns=["fecha"] + list(macro_series.values()))

    df_f4.to_csv(f"{RAW_DIR}/f4_macro_raw.csv", index=False)
    log.info(f"F4 guardado: {len(df_f4)} registros")

    tracker.log("EXTRACT", "F4_macro", len(df_f4), len(df_f4), 0,
                "Indicadores macro: EUR/USD, bonos, oro, petróleo (yfinance)")
    return df_f4


def run_extraction():
    """Ejecuta la extracción completa de las 4 fuentes."""
    os.makedirs(RAW_DIR, exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    tracker = Tracker()

    log.info("══════════════════════════════════════════")
    log.info("  FASE EXTRACT — Pipeline ETL IBEX 35")
    log.info(f"  Periodo: {START_DATE} → {END_DATE}")
    log.info("══════════════════════════════════════════")

    results = {}
    try:
        results["f1"] = extract_f1_precios(tracker)
    except Exception:
        log.error(f"F1 falló:\n{traceback.format_exc()}")

    try:
        results["f2"] = extract_f2_fundamentales(tracker)
    except Exception:
        log.error(f"F2 falló:\n{traceback.format_exc()}")

    try:
        results["f3"] = extract_f3_dividendos(tracker)
    except Exception:
        log.error(f"F3 falló:\n{traceback.format_exc()}")

    try:
        results["f4"] = extract_f4_macro(tracker)
    except Exception:
        log.error(f"F4 falló:\n{traceback.format_exc()}")

    tracker.save()
    log.info("✅ Extracción completada. Tracking guardado.")
    return results


if __name__ == "__main__":
    run_extraction()
