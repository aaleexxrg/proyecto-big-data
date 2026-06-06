"""
transform.py — Construcción del Modelo Dimensional (Esquema en Estrella)
Pipeline ETL IBEX 35 | Introducción a los Sistemas Big Data 2025-2026

Tablas generadas:
  DIM_TIEMPO    — Descomposición temporal
  DIM_EMPRESA   — Dimensión empresa (ticker, nombre, sector, país)
  DIM_INDICADOR — Tipo de dato (precio_cierre, dividendo, etc.)
  DIM_MACRO     — Contexto macroeconómico del día
  FACT_MERCADO  — Tabla de hechos: métricas bursátiles diarias por empresa
"""

import os
import logging
import pandas as pd
import numpy as np

from tracker import Tracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/transform.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

PROCESSED_DIR = "data/processed"
FINAL_DIR     = "data/final"


# ─────────────────────────────────────────────────────────────────────────────
# DIM_TIEMPO
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_tiempo(df_precios: pd.DataFrame) -> pd.DataFrame:
    log.info("  Construyendo DIM_TIEMPO...")
    fechas = pd.to_datetime(df_precios["date"]).dt.normalize().unique()
    df = pd.DataFrame({"fecha": pd.DatetimeIndex(sorted(fechas))})
    df["sk_tiempo"]       = range(1, len(df) + 1)
    df["fecha_date"]      = df["fecha"].dt.date
    df["anio"]            = df["fecha"].dt.year
    df["trimestre"]       = df["fecha"].dt.quarter
    df["mes"]             = df["fecha"].dt.month
    df["nombre_mes"]      = df["fecha"].dt.strftime("%B")
    df["semana"]          = df["fecha"].dt.isocalendar().week.astype(int)
    df["dia"]             = df["fecha"].dt.day
    df["dia_semana"]      = df["fecha"].dt.day_name()
    df["num_dia_semana"]  = df["fecha"].dt.dayofweek  # 0=lunes
    df["es_fin_semana"]   = df["num_dia_semana"].isin([5, 6])
    df["semestre"]        = df["trimestre"].apply(lambda q: 1 if q <= 2 else 2)

    # Festivos nacionales España (aproximación)
    festivos_esp = set()
    for year in df["anio"].unique():
        festivos_esp.update([
            f"{year}-01-01", f"{year}-01-06", f"{year}-04-18", f"{year}-05-01",
            f"{year}-08-15", f"{year}-10-12", f"{year}-11-01", f"{year}-12-06",
            f"{year}-12-08", f"{year}-12-25",
        ])
    df["es_festivo"] = df["fecha"].dt.strftime("%Y-%m-%d").isin(festivos_esp)
    df["fecha_iso"]  = df["fecha"].dt.strftime("%Y-%m-%d")

    cols_final = ["sk_tiempo","fecha_iso","anio","semestre","trimestre","mes",
                  "nombre_mes","semana","dia","dia_semana","num_dia_semana",
                  "es_fin_semana","es_festivo"]
    df = df[cols_final]
    log.info(f"    DIM_TIEMPO: {len(df)} filas")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# DIM_EMPRESA
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_empresa(df_precios: pd.DataFrame,
                      df_fund: pd.DataFrame) -> pd.DataFrame:
    log.info("  Construyendo DIM_EMPRESA...")
    base = df_precios[["ticker","nombre_empresa","sector"]].drop_duplicates()
    fund_cols = ["ticker","market_cap","per","dividend_yield","beta",
                 "price_to_book","country","currency","employees"]
    fund_cols = [c for c in fund_cols if c in df_fund.columns]
    merged = base.merge(df_fund[fund_cols], on="ticker", how="left")
    merged = merged.drop_duplicates(subset=["ticker"])
    merged.insert(0, "sk_empresa", range(1, len(merged) + 1))

    # Capitalización en categorías (small/mid/large cap)
    if "market_cap" in merged.columns:
        def cap_cat(mc):
            if pd.isna(mc):        return "desconocido"
            if mc < 2e9:           return "small_cap"
            elif mc < 10e9:        return "mid_cap"
            else:                  return "large_cap"
        merged["categoria_cap"] = merged["market_cap"].apply(cap_cat)

    log.info(f"    DIM_EMPRESA: {len(merged)} filas")
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# DIM_INDICADOR
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_indicador() -> pd.DataFrame:
    log.info("  Construyendo DIM_INDICADOR...")
    data = [
        (1, "precio_apertura",  "Precio de apertura de la sesión",      "EUR", "continua"),
        (2, "precio_cierre",    "Precio de cierre ajustado",             "EUR", "continua"),
        (3, "precio_maximo",    "Máximo intradía",                       "EUR", "continua"),
        (4, "precio_minimo",    "Mínimo intradía",                       "EUR", "continua"),
        (5, "volumen",          "Número de acciones negociadas",         "unidades", "discreta"),
        (6, "rentabilidad_dia", "Retorno diario porcentual",             "%", "continua"),
        (7, "volatilidad_20d",  "Desviación típica anualizada 20 días",  "%", "continua"),
        (8, "dividendo",        "Importe bruto de dividendo pagado",     "EUR", "continua"),
    ]
    df = pd.DataFrame(data, columns=["sk_indicador","nombre_indicador",
                                      "descripcion","unidad","tipo_variable"])
    log.info(f"    DIM_INDICADOR: {len(df)} filas")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# DIM_MACRO
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_macro(df_macro: pd.DataFrame,
                    dim_tiempo: pd.DataFrame) -> pd.DataFrame:
    log.info("  Construyendo DIM_MACRO...")
    df = df_macro.copy()
    df["fecha_iso"] = pd.to_datetime(df["fecha"]).dt.strftime("%Y-%m-%d")
    merged = dim_tiempo[["sk_tiempo","fecha_iso"]].merge(df, on="fecha_iso", how="left")
    merged.insert(0, "sk_macro", range(1, len(merged) + 1))

    macro_cols = ["sk_macro","sk_tiempo","fecha_iso","eur_usd","bono_eeuu_10y",
                  "futuro_ibex","oro_usd","petroleo_wti","source_id"]
    macro_cols = [c for c in macro_cols if c in merged.columns]
    merged = merged[macro_cols]
    log.info(f"    DIM_MACRO: {len(merged)} filas")
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# FACT_MERCADO
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_mercado(df_precios: pd.DataFrame,
                       df_divs: pd.DataFrame,
                       dim_tiempo: pd.DataFrame,
                       dim_empresa: pd.DataFrame,
                       tracker: Tracker) -> pd.DataFrame:
    log.info("  Construyendo FACT_MERCADO...")

    df = df_precios.copy()
    df["fecha_iso"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    # JOIN con DIM_TIEMPO
    df = df.merge(dim_tiempo[["sk_tiempo","fecha_iso"]], on="fecha_iso", how="left")

    # JOIN con DIM_EMPRESA
    df = df.merge(dim_empresa[["sk_empresa","ticker"]], on="ticker", how="left")

    # Calcular métricas derivadas
    df = df.sort_values(["ticker","fecha_iso"])
    df["rentabilidad_dia"] = (
        df.groupby("ticker")["close"]
          .pct_change()
          .round(6)
    )
    df["volatilidad_20d"] = (
        df.groupby("ticker")["rentabilidad_dia"]
          .transform(lambda g: g.rolling(20, min_periods=5).std() * np.sqrt(252))
          .round(6)
    )
    df["rango_dia"] = (df["high"] - df["low"]).round(4)

    # JOIN con dividendos (left join por ticker + fecha)
    if not df_divs.empty and "fecha_str" in df_divs.columns:
        divs_agg = (df_divs.rename(columns={"fecha_str":"fecha_iso"})
                            .groupby(["ticker","fecha_iso"])["importe_dividendo"]
                            .sum()
                            .reset_index())
        df = df.merge(divs_agg, on=["ticker","fecha_iso"], how="left")
        df["importe_dividendo"] = df["importe_dividendo"].fillna(0.0)
    else:
        df["importe_dividendo"] = 0.0

    # Seleccionar columnas finales
    import datetime as dt
    df["load_timestamp"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["source_id"]      = df.get("source_id", "F1_yfinance_precios")

    fact_cols = [
        "sk_tiempo", "sk_empresa",
        "open", "high", "low", "close", "volume",
        "rentabilidad_dia", "volatilidad_20d", "rango_dia",
        "importe_dividendo",
        "source_id", "load_timestamp"
    ]
    fact_cols = [c for c in fact_cols if c in df.columns]
    fact = df[fact_cols].copy()
    fact.insert(0, "sk_evento", range(1, len(fact) + 1))

    # Verificar sin NaN en FKs
    n_sin_tiempo   = fact["sk_tiempo"].isnull().sum()
    n_sin_empresa  = fact["sk_empresa"].isnull().sum()
    log.info(f"    FACT_MERCADO: {len(fact)} filas | "
             f"sk_tiempo nulos: {n_sin_tiempo} | sk_empresa nulos: {n_sin_empresa}")

    tracker.log("MERGE_FINAL", "FACT_MERCADO", len(df_precios), len(fact),
                len(df_precios) - len(fact),
                "JOIN FACT con DIM_TIEMPO + DIM_EMPRESA + dividendos")
    return fact


# ─────────────────────────────────────────────────────────────────────────────
# EJECUCIÓN PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────

def run_transform():
    os.makedirs(FINAL_DIR, exist_ok=True)
    tracker = Tracker()
    tracker.load()

    log.info("══════════════════════════════════════════")
    log.info("  FASE TRANSFORM — Modelo Dimensional")
    log.info("══════════════════════════════════════════")

    df_precios = pd.read_csv(f"{PROCESSED_DIR}/f1_precios_clean.csv")
    df_fund    = pd.read_csv(f"{PROCESSED_DIR}/f2_fundamentales_clean.csv")
    df_divs    = pd.read_csv(f"{PROCESSED_DIR}/f3_dividendos_clean.csv")
    df_macro   = pd.read_csv(f"{PROCESSED_DIR}/f4_macro_clean.csv")

    dim_tiempo    = build_dim_tiempo(df_precios)
    dim_empresa   = build_dim_empresa(df_precios, df_fund)
    dim_indicador = build_dim_indicador()
    dim_macro     = build_dim_macro(df_macro, dim_tiempo)
    fact_mercado  = build_fact_mercado(df_precios, df_divs, dim_tiempo,
                                       dim_empresa, tracker)

    # Guardar CSV finales
    dim_tiempo.to_csv(   f"{FINAL_DIR}/dim_tiempo.csv",    index=False)
    dim_empresa.to_csv(  f"{FINAL_DIR}/dim_empresa.csv",   index=False)
    dim_indicador.to_csv(f"{FINAL_DIR}/dim_indicador.csv", index=False)
    dim_macro.to_csv(    f"{FINAL_DIR}/dim_macro.csv",     index=False)
    fact_mercado.to_csv( f"{FINAL_DIR}/fact_mercado.csv",  index=False)

    log.info("✅ Modelo dimensional guardado en data/final/")
    for name, df in [("dim_tiempo", dim_tiempo), ("dim_empresa", dim_empresa),
                      ("dim_indicador", dim_indicador), ("dim_macro", dim_macro),
                      ("fact_mercado", fact_mercado)]:
        tracker.log("LOAD_CSV", name, len(df), len(df), 0,
                    f"CSV final exportado a data/final/{name}.csv")

    tracker.save()
    return {
        "dim_tiempo": dim_tiempo, "dim_empresa": dim_empresa,
        "dim_indicador": dim_indicador, "dim_macro": dim_macro,
        "fact_mercado": fact_mercado,
    }


if __name__ == "__main__":
    run_transform()
