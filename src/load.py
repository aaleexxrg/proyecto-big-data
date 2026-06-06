"""
load.py — Carga en Base de Datos MySQL
Pipeline ETL IBEX 35 | Introducción a los Sistemas Big Data 2025-2026

Motor: MySQL / MariaDB
ORM: SQLAlchemy + pandas to_sql
Estrategia: dimensiones primero, FACT al final (integridad referencial)
"""

import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from tracker import Tracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("logs/load.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

FINAL_DIR = "data/final"

# ── Configuración de conexión (cambiar a tus credenciales) ────────────────────
DB_USER   = os.getenv("DB_USER",   "root")
DB_PASS   = os.getenv("DB_PASS",   "tu_password")
DB_HOST   = os.getenv("DB_HOST",   "localhost")
DB_PORT   = os.getenv("DB_PORT",   "3306")
DB_NAME   = os.getenv("DB_NAME",   "ibex35_dw")

# Connection string sin credenciales (para el informe):
# mysql+mysqlconnector://<user>:<pass>@localhost:3306/ibex35_dw
CONNECTION_STRING = (
    f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ── DDL — CREATE TABLE statements ─────────────────────────────────────────────
DDL_STATEMENTS = """
CREATE DATABASE IF NOT EXISTS ibex35_dw
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE ibex35_dw;

-- ── DIMENSIONES ──────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_tiempo (
    sk_tiempo        INT            NOT NULL AUTO_INCREMENT,
    fecha_iso        DATE           NOT NULL,
    anio             SMALLINT       NOT NULL,
    semestre         TINYINT        NOT NULL,
    trimestre        TINYINT        NOT NULL,
    mes              TINYINT        NOT NULL,
    nombre_mes       VARCHAR(20)    NOT NULL,
    semana           TINYINT        NOT NULL,
    dia              TINYINT        NOT NULL,
    dia_semana       VARCHAR(15)    NOT NULL,
    num_dia_semana   TINYINT        NOT NULL COMMENT '0=lunes, 6=domingo',
    es_fin_semana    BOOLEAN        NOT NULL DEFAULT FALSE,
    es_festivo       BOOLEAN        NOT NULL DEFAULT FALSE,
    PRIMARY KEY (sk_tiempo),
    UNIQUE KEY uq_fecha (fecha_iso),
    INDEX idx_anio_mes (anio, mes),
    INDEX idx_trimestre (anio, trimestre)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_empresa (
    sk_empresa       INT            NOT NULL AUTO_INCREMENT,
    ticker           VARCHAR(15)    NOT NULL,
    nombre_empresa   VARCHAR(100)   NOT NULL,
    sector           VARCHAR(50)    NOT NULL,
    country          VARCHAR(50)             DEFAULT 'Spain',
    currency         VARCHAR(10)             DEFAULT 'EUR',
    market_cap       BIGINT,
    per              DECIMAL(8,2),
    per_forward      DECIMAL(8,2),
    dividend_yield   DECIMAL(6,4),
    beta             DECIMAL(6,4),
    price_to_book    DECIMAL(8,4),
    ebitda           BIGINT,
    revenue          BIGINT,
    profit_margin    DECIMAL(8,6),
    employees        INT,
    categoria_cap    VARCHAR(20),
    PRIMARY KEY (sk_empresa),
    UNIQUE KEY uq_ticker (ticker),
    INDEX idx_sector (sector)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_indicador (
    sk_indicador     INT            NOT NULL AUTO_INCREMENT,
    nombre_indicador VARCHAR(50)    NOT NULL,
    descripcion      VARCHAR(200),
    unidad           VARCHAR(20),
    tipo_variable    VARCHAR(20),
    PRIMARY KEY (sk_indicador)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS dim_macro (
    sk_macro         INT            NOT NULL AUTO_INCREMENT,
    sk_tiempo        INT            NOT NULL,
    fecha_iso        DATE           NOT NULL,
    eur_usd          DECIMAL(10,6),
    bono_eeuu_10y    DECIMAL(8,4),
    futuro_ibex      DECIMAL(12,2),
    oro_usd          DECIMAL(10,4),
    petroleo_wti     DECIMAL(10,4),
    source_id        VARCHAR(100),
    PRIMARY KEY (sk_macro),
    INDEX idx_sk_tiempo_macro (sk_tiempo),
    CONSTRAINT fk_macro_tiempo FOREIGN KEY (sk_tiempo)
        REFERENCES dim_tiempo(sk_tiempo) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ── TABLA DE HECHOS ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_mercado (
    sk_evento          BIGINT         NOT NULL AUTO_INCREMENT,
    sk_tiempo          INT            NOT NULL,
    sk_empresa         INT            NOT NULL,
    open               DECIMAL(12,4),
    high               DECIMAL(12,4),
    low                DECIMAL(12,4),
    close              DECIMAL(12,4)  NOT NULL,
    volume             BIGINT,
    rentabilidad_dia   DECIMAL(10,6),
    volatilidad_20d    DECIMAL(10,6),
    rango_dia          DECIMAL(10,4),
    importe_dividendo  DECIMAL(10,4)  DEFAULT 0.0000,
    source_id          VARCHAR(100),
    load_timestamp     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sk_evento),
    INDEX idx_tiempo    (sk_tiempo),
    INDEX idx_empresa   (sk_empresa),
    INDEX idx_cierre    (close),
    INDEX idx_fecha_empresa (sk_tiempo, sk_empresa),
    CONSTRAINT fk_fact_tiempo   FOREIGN KEY (sk_tiempo)
        REFERENCES dim_tiempo(sk_tiempo)   ON DELETE RESTRICT,
    CONSTRAINT fk_fact_empresa  FOREIGN KEY (sk_empresa)
        REFERENCES dim_empresa(sk_empresa) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def create_database_schema(engine):
    """Crea el esquema de tablas si no existe."""
    log.info("  Creando esquema de base de datos...")
    with engine.connect() as conn:
        for statement in DDL_STATEMENTS.strip().split(";"):
            s = statement.strip()
            if s:
                try:
                    conn.execute(text(s))
                    conn.commit()
                except Exception as e:
                    log.warning(f"  DDL omitido (posiblemente ya existe): {e}")
    log.info("  ✅ Esquema creado/verificado")


def load_table(engine, df: pd.DataFrame, table_name: str,
               tracker: Tracker, chunksize: int = 5000):
    """Carga un DataFrame en MySQL usando pandas.to_sql."""
    log.info(f"  Cargando {table_name}: {len(df)} filas...")
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="append",
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        # Verificar carga
        with engine.connect() as conn:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        log.info(f"  ✅ {table_name}: {count} filas en BD")
        tracker.log("LOAD_SQL", table_name, len(df), int(count),
                    len(df) - int(count), f"MySQL → {DB_NAME}.{table_name}")
    except Exception as e:
        log.error(f"  ✗ Error cargando {table_name}: {e}")
        tracker.log("LOAD_SQL_ERROR", table_name, len(df), 0, len(df), str(e))


def run_load():
    tracker = Tracker()
    tracker.load()

    log.info("══════════════════════════════════════════")
    log.info("  FASE LOAD — Carga MySQL ibex35_dw")
    log.info("══════════════════════════════════════════")

    # Leer CSVs finales
    dim_tiempo    = pd.read_csv(f"{FINAL_DIR}/dim_tiempo.csv")
    dim_empresa   = pd.read_csv(f"{FINAL_DIR}/dim_empresa.csv")
    dim_indicador = pd.read_csv(f"{FINAL_DIR}/dim_indicador.csv")
    dim_macro     = pd.read_csv(f"{FINAL_DIR}/dim_macro.csv")
    fact_mercado  = pd.read_csv(f"{FINAL_DIR}/fact_mercado.csv")

    # Castear tipos problemáticos antes de carga
    dim_tiempo["fecha_iso"]    = pd.to_datetime(dim_tiempo["fecha_iso"]).dt.date
    dim_tiempo["es_fin_semana"]= dim_tiempo["es_fin_semana"].astype(bool)
    dim_tiempo["es_festivo"]   = dim_tiempo["es_festivo"].astype(bool)
    dim_macro["fecha_iso"]     = pd.to_datetime(dim_macro["fecha_iso"]).dt.date
    fact_mercado["load_timestamp"] = pd.to_datetime(fact_mercado["load_timestamp"])

    engine = create_engine(CONNECTION_STRING, echo=False)

    create_database_schema(engine)

    # Cargar dimensiones primero (orden importante por FK)
    load_table(engine, dim_tiempo,    "dim_tiempo",    tracker)
    load_table(engine, dim_empresa,   "dim_empresa",   tracker)
    load_table(engine, dim_indicador, "dim_indicador", tracker)
    load_table(engine, dim_macro,     "dim_macro",     tracker)

    # Cargar FACT al final
    load_table(engine, fact_mercado,  "fact_mercado",  tracker)

    tracker.save()
    log.info("✅ Carga completada en MySQL ibex35_dw")


if __name__ == "__main__":
    run_load()
