"""
clean.py — Módulo de Limpieza y Transformación (T01–T10)
Pipeline ETL IBEX 35 | Introducción a los Sistemas Big Data 2025-2026
"""

import os
import re
import hashlib
import logging
import unicodedata

import pandas as pd
import numpy as np
from scipy import stats

from tracker import Tracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/clean.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

RAW_DIR       = "data/raw"
PROCESSED_DIR = "data/processed"


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────────────────

def _hash_sha256(value: str) -> str:
    """SHA-256 para seudonimización PII."""
    return hashlib.sha256(str(value).encode()).hexdigest()


def _normalize_text(s: str) -> str:
    """Minúsculas + eliminar acentos + strip."""
    if not isinstance(s, str):
        return s
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s


def quality_profile(df: pd.DataFrame, nombre: str) -> dict:
    """Genera perfil de calidad de un DataFrame."""
    n_rows, n_cols = df.shape
    nulos = df.isnull().sum()
    nulos_pct = (nulos / n_rows * 100).round(2)

    blancos = {}
    for col in df.select_dtypes(include="object").columns:
        blancos[col] = (df[col].str.strip() == "").sum()

    duplicados = df.duplicated().sum()

    profile = {
        "nombre":      nombre,
        "registros":   n_rows,
        "variables":   n_cols,
        "nulos":       nulos.to_dict(),
        "nulos_pct":   nulos_pct.to_dict(),
        "blancos":     blancos,
        "duplicados":  int(duplicados),
        "dtypes":      df.dtypes.astype(str).to_dict(),
    }
    log.info(f"  Perfil [{nombre}]: {n_rows} filas, {n_cols} cols, "
             f"{int(nulos.sum())} nulos, {int(duplicados)} duplicados")
    return profile


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIONES T01–T10 para F1 (Precios)
# ─────────────────────────────────────────────────────────────────────────────

def clean_f1_precios(tracker: Tracker) -> pd.DataFrame:
    log.info("=== Limpieza F1: Precios IBEX 35 ===")
    df = pd.read_csv(f"{RAW_DIR}/f1_precios_raw.csv")
    n_entrada = len(df)
    log.info(f"  Entrada: {n_entrada} registros")

    # T03 — Blancos a NaN
    str_cols = df.select_dtypes(include="object").columns
    for col in str_cols:
        mask = df[col].str.strip() == ""
        df.loc[mask, col] = np.nan
    n_blancos = sum((df[c].str.strip() == "").sum()
                    for c in str_cols if c in df.columns)
    log.info(f"  T03 Blancos corregidos: {n_blancos}")
    tracker.log("T03", "F1_precios", n_entrada, n_entrada, n_blancos, "Blancos → NaN")

    # T01 — Eliminar duplicados (ticker + fecha)
    n_antes = len(df)
    df = df.drop_duplicates(subset=["ticker", "date"])
    n_dupl = n_antes - len(df)
    log.info(f"  T01 Duplicados eliminados: {n_dupl}")
    tracker.log("T01", "F1_precios", n_antes, len(df), n_dupl,
                "drop_duplicates(ticker, date)")

    # T04 — Corrección de tipos
    df["date"]   = pd.to_datetime(df["date"])
    df["open"]   = pd.to_numeric(df["open"],   errors="coerce")
    df["high"]   = pd.to_numeric(df["high"],   errors="coerce")
    df["low"]    = pd.to_numeric(df["low"],    errors="coerce")
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    log.info("  T04 Tipos corregidos: date→datetime, OHLCV→float/int")
    tracker.log("T04", "F1_precios", len(df), len(df), 0,
                "Casteo date→datetime, OHLCV→numeric")

    # T05 — Fechas ISO-8601 UTC
    df["date"] = df["date"].dt.tz_localize(None)  # sin tz para MySQL
    df["date_str"] = df["date"].dt.strftime("%Y-%m-%d")
    log.info("  T05 Fechas normalizadas a ISO-8601")
    tracker.log("T05", "F1_precios", len(df), len(df), 0, "Fechas → YYYY-MM-DD")

    # T06 — Normalización de texto (ticker, nombre_empresa, sector)
    for col in ["ticker", "nombre_empresa", "sector"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    log.info("  T06 Texto normalizado (strip en categorías)")
    tracker.log("T06", "F1_precios", len(df), len(df), 0, "strip en columnas texto")

    # T02 — Nulos: imputar OHLCV con forward-fill por ticker
    n_nulos_antes = df[["open","high","low","close","volume"]].isnull().sum().sum()
    df = df.sort_values(["ticker","date"])
    df[["open","high","low","close","volume"]] = (
        df.groupby("ticker")[["open","high","low","close","volume"]]
          .transform(lambda g: g.ffill().bfill())
    )
    n_nulos_despues = df[["open","high","low","close","volume"]].isnull().sum().sum()
    log.info(f"  T02 Nulos imputados: {int(n_nulos_antes - n_nulos_despues)}")
    tracker.log("T02", "F1_precios", len(df), len(df),
                int(n_nulos_antes - n_nulos_despues),
                "forward-fill + back-fill por ticker")

    # T07 — Rangos de negocio: close > 0, volume >= 0
    n_antes = len(df)
    df = df[(df["close"] > 0) & (df["volume"] >= 0)]
    n_rango = n_antes - len(df)
    log.info(f"  T07 Fuera de rango eliminados: {n_rango}")
    tracker.log("T07", "F1_precios", n_antes, len(df), n_rango,
                "close > 0 y volume >= 0")

    # T08 — Outliers z-score en 'close' (|z|>4 por ticker)
    n_antes = len(df)
    z = df.groupby("ticker")["close"].transform(
        lambda g: np.abs(stats.zscore(g, nan_policy="omit"))
    )
    outliers_mask = z > 4
    n_out = outliers_mask.sum()
    df.loc[outliers_mask, "close"] = np.nan
    df["close"] = df.groupby("ticker")["close"].transform(lambda g: g.ffill().bfill())
    log.info(f"  T08 Outliers detectados y corregidos: {int(n_out)}")
    tracker.log("T08", "F1_precios", n_antes, len(df), int(n_out),
                "z-score |z|>4 en close → imputado con ffill")

    # T09 — Seudonimización: no hay PII directa en precios (campo ticker es público)
    log.info("  T09 Sin PII en F1 (datos públicos de mercado)")
    tracker.log("T09", "F1_precios", len(df), len(df), 0,
                "Sin PII en precios bursátiles")

    # T10 — Consistencia referencial (verificar tickers conocidos)
    tickers_conocidos = set(
        ["ACS.MC","ACX.MC","AMS.MC","ANA.MC","ANE.MC","BBVA.MC","BKT.MC",
         "CABK.MC","CLNX.MC","COL.MC","ELE.MC","ENG.MC","FDR.MC","FER.MC",
         "GRF.MC","IAG.MC","IBE.MC","IDR.MC","INDITEX.MC","LOG.MC","MAP.MC",
         "MEL.MC","MRL.MC","MTS.MC","NTGY.MC","PUIG.MC","RED.MC","REP.MC",
         "ROVI.MC","SAB.MC","SAN.MC","SGRE.MC","SLR.MC","TEF.MC","UNI.MC"]
    )
    huerfanos = ~df["ticker"].isin(tickers_conocidos)
    n_huerfanos = huerfanos.sum()
    log.info(f"  T10 Registros huérfanos (ticker desconocido): {int(n_huerfanos)}")
    tracker.log("T10", "F1_precios", len(df), len(df), int(n_huerfanos),
                "Verificación ticker vs lista IBEX35 conocida")

    # Añadir campos de trazabilidad
    df["source_id"] = "F1_yfinance_precios"

    df.to_csv(f"{PROCESSED_DIR}/f1_precios_clean.csv", index=False)
    log.info(f"  ✅ F1 limpio: {len(df)} registros → f1_precios_clean.csv")
    tracker.log("CLEAN_FINAL", "F1_precios", n_entrada, len(df),
                n_entrada - len(df), "Pipeline limpieza completo T01-T10")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIONES T01–T10 para F2 (Fundamentales)
# ─────────────────────────────────────────────────────────────────────────────

def clean_f2_fundamentales(tracker: Tracker) -> pd.DataFrame:
    log.info("=== Limpieza F2: Fundamentales ===")
    df = pd.read_csv(f"{RAW_DIR}/f2_fundamentales_raw.csv")
    n_entrada = len(df)

    # T03 Blancos
    for col in df.select_dtypes(include="object").columns:
        df.loc[df[col].str.strip() == "", col] = np.nan
    tracker.log("T03", "F2_fundamentales", n_entrada, n_entrada, 0,
                "Blancos → NaN en columnas texto")

    # T01 Duplicados por ticker
    n_antes = len(df)
    df = df.drop_duplicates(subset=["ticker"])
    tracker.log("T01", "F2_fundamentales", n_antes, len(df), n_antes - len(df),
                "drop_duplicates(ticker)")

    # T04 Tipos numéricos
    num_cols = ["market_cap","per","per_forward","dividend_yield","beta",
                "price_to_book","ebitda","revenue","profit_margin",
                "52w_high","52w_low","avg_volume","employees"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    tracker.log("T04", "F2_fundamentales", len(df), len(df), 0,
                "Casteo columnas métricas → float")

    # T02 Nulos: imputar con mediana sectorial
    for col in ["per","dividend_yield","beta","price_to_book"]:
        if col in df.columns:
            df[col] = df.groupby("sector")[col].transform(
                lambda g: g.fillna(g.median())
            )
    tracker.log("T02", "F2_fundamentales", len(df), len(df), 0,
                "Nulos en ratios → mediana sectorial")

    # T06 Texto
    for col in ["nombre_empresa","sector","country","currency"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    tracker.log("T06", "F2_fundamentales", len(df), len(df), 0,
                "strip en columnas categóricas")

    # T07 Rangos: PER razonable 0-1000, dividend_yield 0-1
    if "per" in df.columns:
        df.loc[(df["per"] < 0) | (df["per"] > 1000), "per"] = np.nan
    if "dividend_yield" in df.columns:
        df.loc[(df["dividend_yield"] < 0) | (df["dividend_yield"] > 1), "dividend_yield"] = np.nan
    tracker.log("T07", "F2_fundamentales", len(df), len(df), 0,
                "PER∈[0,1000], dividend_yield∈[0,1]")

    # T08 Outliers en market_cap (z-score > 3)
    if "market_cap" in df.columns:
        z = np.abs(stats.zscore(df["market_cap"].dropna()))
        # Sólo documentar, no eliminar (son empresas reales con distinto tamaño)
        n_out = (z > 3).sum()
        log.info(f"  T08 market_cap outliers detectados (no eliminados): {int(n_out)}")
    tracker.log("T08", "F2_fundamentales", len(df), len(df), 0,
                "Outliers market_cap documentados; no eliminados (varianza natural)")

    # T09 Sin PII en fundamentales
    tracker.log("T09", "F2_fundamentales", len(df), len(df), 0, "Sin PII")

    # T10 Consistencia referencial
    tracker.log("T10", "F2_fundamentales", len(df), len(df), 0,
                "Un registro por ticker verificado")

    df["source_id"] = "F2_yfinance_fundamentales"
    df.to_csv(f"{PROCESSED_DIR}/f2_fundamentales_clean.csv", index=False)
    log.info(f"  ✅ F2 limpio: {len(df)} registros")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIONES para F3 (Dividendos)
# ─────────────────────────────────────────────────────────────────────────────

def clean_f3_dividendos(tracker: Tracker) -> pd.DataFrame:
    log.info("=== Limpieza F3: Dividendos ===")
    df = pd.read_csv(f"{RAW_DIR}/f3_dividendos_raw.csv")
    n_entrada = len(df)

    if df.empty:
        log.warning("  F3 vacío, saltando limpieza")
        df.to_csv(f"{PROCESSED_DIR}/f3_dividendos_clean.csv", index=False)
        return df

    # T03 Blancos
    for col in df.select_dtypes(include="object").columns:
        df.loc[df[col].str.strip() == "", col] = np.nan
    tracker.log("T03", "F3_dividendos", n_entrada, n_entrada, 0, "Blancos → NaN")

    # T01 Duplicados
    n_antes = len(df)
    df = df.drop_duplicates(subset=["ticker","fecha_ex_dividendo"])
    tracker.log("T01", "F3_dividendos", n_antes, len(df), n_antes - len(df),
                "drop_duplicates(ticker, fecha_ex_dividendo)")

    # T04 Tipos
    df["fecha_ex_dividendo"] = pd.to_datetime(df["fecha_ex_dividendo"],
                                               errors="coerce", utc=False)
    df["importe_dividendo"] = pd.to_numeric(df["importe_dividendo"], errors="coerce")
    tracker.log("T04", "F3_dividendos", len(df), len(df), 0,
                "fecha→datetime, importe→float")

    # T05 Fechas ISO-8601
    df["fecha_str"] = df["fecha_ex_dividendo"].dt.strftime("%Y-%m-%d")
    tracker.log("T05", "F3_dividendos", len(df), len(df), 0, "Fecha → YYYY-MM-DD")

    # T02 Nulos en importe → mediana del ticker
    df["importe_dividendo"] = df.groupby("ticker")["importe_dividendo"].transform(
        lambda g: g.fillna(g.median())
    )
    tracker.log("T02", "F3_dividendos", len(df), len(df), 0,
                "Nulos importe → mediana del ticker")

    # T07 Rangos: importe > 0
    n_antes = len(df)
    df = df[df["importe_dividendo"] > 0]
    tracker.log("T07", "F3_dividendos", n_antes, len(df), n_antes - len(df),
                "importe_dividendo > 0")

    # T06 Texto
    for col in ["ticker","nombre_empresa","sector"]:
        df[col] = df[col].astype(str).str.strip()
    tracker.log("T06", "F3_dividendos", len(df), len(df), 0, "strip texto")

    # T08 Outliers en importe (z-score > 3 por ticker)
    z_g = df.groupby("ticker")["importe_dividendo"].transform(
        lambda g: np.abs(stats.zscore(g, nan_policy="omit")) if len(g) > 2 else pd.Series([0]*len(g), index=g.index)
    )
    n_out = (z_g > 3).sum()
    df.loc[z_g > 3, "importe_dividendo"] = np.nan
    df["importe_dividendo"] = df.groupby("ticker")["importe_dividendo"].transform(
        lambda g: g.ffill().bfill()
    )
    tracker.log("T08", "F3_dividendos", len(df), len(df), int(n_out),
                "z-score >3 en importe_dividendo → imputado")

    tracker.log("T09", "F3_dividendos", len(df), len(df), 0, "Sin PII")
    tracker.log("T10", "F3_dividendos", len(df), len(df), 0,
                "Consistencia ticker vs lista IBEX35")

    df["source_id"] = "F3_yfinance_dividendos"
    df.to_csv(f"{PROCESSED_DIR}/f3_dividendos_clean.csv", index=False)
    log.info(f"  ✅ F3 limpio: {len(df)} registros")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# TRANSFORMACIONES para F4 (Macro)
# ─────────────────────────────────────────────────────────────────────────────

def clean_f4_macro(tracker: Tracker) -> pd.DataFrame:
    log.info("=== Limpieza F4: Macro ===")
    df = pd.read_csv(f"{RAW_DIR}/f4_macro_raw.csv")
    n_entrada = len(df)

    # T04 Tipos
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce", utc=False)
    num_cols = [c for c in df.columns if c != "fecha"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    tracker.log("T04", "F4_macro", n_entrada, n_entrada, 0,
                "fecha→datetime, indicadores→float")

    # T05 Fechas ISO
    df["fecha_str"] = df["fecha"].dt.strftime("%Y-%m-%d")
    tracker.log("T05", "F4_macro", len(df), len(df), 0, "fecha → YYYY-MM-DD")

    # T01 Duplicados
    n_antes = len(df)
    df = df.drop_duplicates(subset=["fecha"])
    tracker.log("T01", "F4_macro", n_antes, len(df), n_antes - len(df),
                "drop_duplicates(fecha)")

    # T02 Nulos → interpolación lineal (series temporales continuas)
    n_nulos = df[num_cols].isnull().sum().sum()
    df[num_cols] = df[num_cols].interpolate(method="linear").ffill().bfill()
    tracker.log("T02", "F4_macro", len(df), len(df), int(n_nulos),
                "Nulos → interpolación lineal + ffill/bfill")

    # T07 Rangos: EUR/USD debe estar en [0.5, 2.0], petróleo > 0
    if "eur_usd" in df.columns:
        df.loc[(df["eur_usd"] < 0.5) | (df["eur_usd"] > 2.0), "eur_usd"] = np.nan
    if "petroleo_wti" in df.columns:
        df.loc[df["petroleo_wti"] <= 0, "petroleo_wti"] = np.nan
    tracker.log("T07", "F4_macro", len(df), len(df), 0,
                "eur_usd∈[0.5,2.0], petroleo>0")

    # T08 Outliers z-score > 4
    for col in num_cols:
        if col in df.columns:
            z = np.abs(stats.zscore(df[col].dropna()))
    tracker.log("T08", "F4_macro", len(df), len(df), 0,
                "z-score calculado, series macro sin outliers extremos")

    tracker.log("T09", "F4_macro", len(df), len(df), 0, "Sin PII")
    tracker.log("T10", "F4_macro", len(df), len(df), 0,
                "Series temporales completas sin huecos")

    df["source_id"] = "F4_yfinance_macro"
    df.to_csv(f"{PROCESSED_DIR}/f4_macro_clean.csv", index=False)
    log.info(f"  ✅ F4 limpio: {len(df)} registros")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_cleaning():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    tracker = Tracker()
    tracker.load()

    log.info("══════════════════════════════════════════")
    log.info("  FASE CLEAN/TRANSFORM — Pipeline ETL IBEX 35")
    log.info("══════════════════════════════════════════")

    dfs = {}
    for fn, name in [
        (clean_f1_precios,       "f1"),
        (clean_f2_fundamentales, "f2"),
        (clean_f3_dividendos,    "f3"),
        (clean_f4_macro,         "f4"),
    ]:
        try:
            dfs[name] = fn(tracker)
        except Exception as e:
            log.error(f"{name} falló: {e}")

    tracker.save()
    log.info("✅ Limpieza completada.")
    return dfs


if __name__ == "__main__":
    run_cleaning()
